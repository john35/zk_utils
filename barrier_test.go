package zk_utils

import (
	"fmt"
	"github.com/go-zookeeper/zk"
	mathrand "math/rand"
	"testing"
	"time"
)

func TestNewBarrier(t *testing.T) {
	conn, _, err := zk.Connect([]string{"127.0.0.1:2181", "127.0.0.1:2182", "127.0.0.1:2183"}, time.Second*15)
	if err != nil {
		t.Fatal(err)
		return
	}
	b, err := NewBarrier(conn, "/test/t1", 10)
	if err != nil {
		t.Fatal(err)
		return
	}

	var f = func(name string) {
		if ok, err := b.Enter(name); err != nil || !ok {
			return
		}
		t.Log(name, "has enter barrier,doing something")
		s := mathrand.NewSource(time.Now().UnixNano())
		r := mathrand.New(s)
		n := r.Int31n(10)
		fmt.Printf("%s begin sleep %d seconds\n", name, n)
		time.Sleep(time.Duration(n) * time.Second)
		t.Log(name, "begin to leave barrier,doing something")
		if ok, err := b.Leave(name); err != nil || !ok {
			return
		}
		t.Log(name, "has left barrier")
	}

	for i := 0; i < 200; i++ {
		go f(fmt.Sprintf("test_%d", i))
	}

	var ch chan int

	ch <- 1
}
