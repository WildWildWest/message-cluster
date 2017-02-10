package main

import (
	"fmt"
	"github.com/docker/docker/pkg/namesgenerator"
	"github.com/garyburd/redigo/redis"
	"net/http"
	"time"
)

type ClusterMember struct {
	Name    string `redis:"name"`
	Address string `redis:"address"`
	Port    string `redis:"port"`
	Prev    string `redis:"prev"`
	Next    string `redis:"next"`
	Leader  bool   `redis:"leader"`
}

type Cluster struct {
	Current string
	Pool    *redis.Pool
	Cfg     ClusterConfig
}

type ClusterConfig struct {
	Protocol           string
	First              string
	Last               string
	AliveInterval      time.Duration
	NextRequestTimeout time.Duration
	MsgPromoted        string
}

//Подключение к кластеру
//Кластер выстроен в виде кольца, где каждый элемент содержит указатель на следующий и предыдущий. Первый и последний элементы указывают друг на друга.
func (c *Cluster) Join(addr string, port *int, name string) {
	fmt.Println("instance name: ", name)
	ExecTrans(c.Pool, func(cl redis.Conn) {
		var leader int
		f, _ := redis.String(cl.Do("GET", c.Cfg.First))
		l, _ := redis.String(cl.Do("GET", c.Cfg.Last))

		cl.Do("MULTI")

		if len(l) == 0 {
			cl.Do("SET", c.Cfg.First, name)
			leader = 1
		} else {
			cl.Do("HSET", "cluster:"+l, "next", name)
			cl.Do("HSET", "cluster:"+f, "prev", name)
		}
		cl.Do("HMSET", "cluster:"+name, "name", name, "address", addr, "port", *port, "prev", l, "next", f, "leader", leader)
		c.Current = name
		cl.Do("SET", c.Cfg.Last, name)
	}, c.Cfg.Last, c.Cfg.First)
}

//Удаление из кластера
//При удалении элемента из кластера указатели соседних элементов будут указывать дург на друга
func (c Cluster) Remove(name string) {
	ExecTrans(c.Pool, func(cl redis.Conn) {
		f, _ := redis.String(cl.Do("GET", c.Cfg.First))
		l, _ := redis.String(cl.Do("GET", c.Cfg.Last))
		m := c.GetMember(name)

		cl.Do("MULTI")

		if name == f {
			cl.Do("SET", c.Cfg.First, m.Next)
		}
		if name == f && len(m.Next) > 0 {
			cl.Do("HSET", "cluster:"+m.Next, "leader", 1)
			cl.Do("PUBLISH", "messages.main."+m.Next, c.Cfg.MsgPromoted)
			fmt.Println("Member '"+m.Next+"' being promoted to leader")
		}
		if name == l {
			cl.Do("SET", c.Cfg.Last, m.Prev)
		}
		if m.Prev == m.Next && len(m.Next) > 0 {
			cl.Do("HSET", "cluster:"+m.Prev, "next", "")
			cl.Do("HSET", "cluster:"+m.Next, "prev", "")
		} else if m.Prev != m.Next {
			cl.Do("HSET", "cluster:"+m.Prev, "next", m.Next)
			cl.Do("HSET", "cluster:"+m.Next, "prev", m.Prev)
		}
		cl.Do("DEL", "cluster:"+name)
	}, c.Cfg.Last, c.Cfg.First, "cluster:"+name)
}

//Получение информации о члене кластера. Данные записываются в структуру ClusterMember
func (c Cluster) GetMember(name string) ClusterMember {
	if len(name) == 0 {
		return ClusterMember{}
	}
	cl := c.Pool.Get()
	defer cl.Close()
	var m ClusterMember
	v, _ := redis.Values(cl.Do("HGETALL", "cluster:"+name))
	redis.ScanStruct(v, &m)

	return m
}

//Проверяем отвечает ли соседний член класера, если нет удаляем его из кластера
func (c Cluster) CheckNext() {
	cur := c.GetMember(c.Current)
	if len(cur.Next) > 0 {
		n := c.GetMember(cur.Next)
		url := fmt.Sprintf("%s://%s:%s/is_alive", c.Cfg.Protocol, n.Address, n.Port)

		cl := http.Client{
			Timeout: c.Cfg.NextRequestTimeout,
		}
		_, err := cl.Head(url)
		if err != nil {
			fmt.Println("Error: ", err)
			c.Remove(n.Name)
		}
	}
}

//Удаляем запись, которая осталась после завершения последнего экземпляра приложения
func (c Cluster) ClearFirstMember(address string, port string) {
	cl := c.Pool.Get()
	defer cl.Close()

	f, _ := redis.String(cl.Do("GET", c.Cfg.First))
	m := c.GetMember(f)

	if m.Address == address && m.Port == port {
		c.Remove(f)
	}
}

//В фоне проверяем каждые 100 мс отвечает ли соседний член кластера
func (c Cluster) CheckHealth() {
	for {
		time.Sleep(c.Cfg.AliveInterval)
		go c.CheckNext()
	}
}

//TODO: обернуть в транзакцию
//Каждый член кластера получает уникальное имя
func (c Cluster) GetRandomName() string {
	begin:
	name := namesgenerator.GetRandomName(0)

	m := c.GetMember(name)
	if len(m.Name) > 0 {
		goto begin
	}

	return name
}
