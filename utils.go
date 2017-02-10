package main

import "github.com/garyburd/redigo/redis"

//Декоратор для транзакций.
func ExecTrans(p *redis.Pool, f func(cl redis.Conn), w ...interface{}) {
	c := p.Get()
	defer c.Close()
	for {
		c.Do("WATCH", w...)
		f(c)
		queued, _ := c.Do("EXEC")

		if queued != nil {
			break
		}
	}
}