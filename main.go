package main

import (
	"flag"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"math/rand"
	"net/http"
	"strconv"
	"time"
)

//создаем пул подключений, поскольку одно подключение не позволяет конкурентно выполнять команды
func NewRedisPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", ":6379") },
	}
}

//Обращаясь к этой страница проверяем доступен ли данный член кластера
func isAlive(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "1")
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {
	var (
		protocol           = flag.String("protocol", "http", "App protocol")
		port               = flag.Int("port", 8080, "App port")
		getErrors          = flag.Int("getErrors", 0, "Print errors")
		addr               = flag.String("address", "127.0.0.1", "App address")
		clusterFirst       = flag.String("clusterFirst", "cluster:_first", "Redis variable to save first element of cluster")
		clusterLast        = flag.String("clusterLast", "cluster:_last", "Redis variable to save  last element of cluster")
		msgInterval        = flag.Duration("msgInterval", 500*time.Millisecond, "Interval between sending messages")
		aliveInterval      = flag.Duration("aliveInterval", 100*time.Millisecond, "Interval between alive checks")
		nextRequestTimeout = flag.Duration("nextTimeout", 200*time.Millisecond, "Timeout between checking next member of cluster")
		wrongMessages      = flag.String("wrongMessages", "wrong_messages", "Redis variable to save wrong messages")
		msgPromoted        = flag.String("msgPromoted", "status_promoted", "Promotion message of app")
		pool               = NewRedisPool()
	)
	flag.Parse()
	m := Messages{
		Pool: pool,
		Cfg: MessagesConfig{
			SendInterval:  *msgInterval,
			WrongMessages: *wrongMessages,
		},
	}
	if *getErrors == 1 {
		m.PrintWrong()
		return
	}
	c := Cluster{
		Pool: pool,
		Cfg: ClusterConfig{
			Protocol:           *protocol,
			First:              *clusterFirst,
			Last:               *clusterLast,
			AliveInterval:      *aliveInterval,
			NextRequestTimeout: *nextRequestTimeout,
			MsgPromoted:        *msgPromoted,
		},
	}
	c.ClearFirstMember(*addr, strconv.Itoa(*port))
	c.Join(*addr, port, c.GetRandomName())

	go c.CheckHealth()
	go m.Process(c)

	http.HandleFunc("/is_alive", isAlive)
	http.ListenAndServe(*addr+":"+strconv.Itoa(*port), nil)
}
