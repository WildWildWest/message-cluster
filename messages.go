package main

import 	(
	"github.com/garyburd/redigo/redis"
	"fmt"
	"time"
	"math/rand"
)


type Messages struct {
	Pool 			*redis.Pool
	Cfg			MessagesConfig
}

type MessagesConfig struct {
	SendInterval 		time.Duration
	WrongMessages		string
}

//Генерируем или получаем сообщения в зависимости от роли данного члена класера
func (m Messages) Process(c Cluster) {
	cl := m.Pool.Get()
	defer cl.Close()

	if cm := c.GetMember(c.Current); cm.Leader == true {
		go m.Gen(c)
		return
	}

	go m.Read(c)

}

//Читаем и проверяем полученные сообщения
func (m Messages) Read(c Cluster) {
	psc := redis.PubSubConn{Conn: m.Pool.Get()}
	psc.Subscribe("messages.main."+c.Current)
	defer psc.Conn.Close()

	for {
		switch msg := psc.Receive().(type) {
		case redis.Message:
			s := string(msg.Data[:])
			if s == c.Cfg.MsgPromoted {
				go m.Gen(c)
				return
			}
			fmt.Printf("Message: %s %s\n", msg.Channel, msg.Data)
			m.Check(s)
		}
	}
}

//Генерируем сообщения для остальных членов кластера
func (m Messages) Gen(c Cluster) {
	cl := m.Pool.Get()
	defer cl.Close()
	cur := c.GetMember(c.Current)
	begin:
	for ;len(cur.Next) == 0; cur = c.GetMember(c.Current) {
		time.Sleep(100 * time.Millisecond)
	}
	n := c.GetMember(cur.Next)
	for {
		time.Sleep(m.Cfg.SendInterval)
		if len(n.Name) > 0 && n.Name != cur.Name {
			msg := m.RandMsg(20)
			cl.Do("PUBLISH", "messages.main." + n.Name, msg)
			fmt.Println("reciver: ", n.Name, "message: ", msg)
		}
		if n = c.GetMember(n.Next); len(n.Next) == 0 {
			goto begin
		}
	}
}

//В пяти процентах случаев добавляем сообщение в лог сообщений с ошибками
func (m Messages) Check(message string) {
	if(rand.Intn(20) != 1) {
		return
	}
	m.LogWrong(message)
}

//Записываем ошибку в Redis
func (m Messages) LogWrong(msg string) {
	cl := m.Pool.Get()
	defer cl.Close()

	cl.Do("RPUSH", m.Cfg.WrongMessages, msg)
	fmt.Println("Wrong message:" + msg)
}

//Получаем список сообщений с ошибками
func (m Messages) GetWrong() []string {
	var res []string
	ExecTrans(m.Pool, func(cl redis.Conn) {
		res, _ = redis.Strings(cl.Do("LRANGE", m.Cfg.WrongMessages, 0, -1))
		cl.Do("MULTI")
		cl.Do("DEL", m.Cfg.WrongMessages)
	}, m.Cfg.WrongMessages)

	return res
}

//Выводим список сообщений с ошибками
func (m Messages) PrintWrong() {
	errors := m.GetWrong()
	if len(errors) == 0 {
		fmt.Println("No wrong messages")
		return
	}
	fmt.Println("Wrong messages:")
	for _, e := range errors {
		fmt.Println("-" + e)
	}
}

//Генерируем случайное сообщение
func (m Messages) RandMsg(n int) string {
	var r = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = r[rand.Intn(len(r))]
	}
	return string(b)
}