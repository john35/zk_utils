package zk_utils

import (
	"fmt"
	"github.com/go-zookeeper/zk"
	"testing"
	"time"
)

func TestProduce(t *testing.T) {
	conn, _, err := zk.Connect([]string{"127.0.0.1:2181", "127.0.0.1:2182", "127.0.0.1:2183"}, time.Second*15)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	queue, err := NewQueue(conn, "/test/queue")
	if err != nil {
		t.Fatal(err)
		return
	}
	for i := 0; i < 100; i++ {
		queue.Producer(fmt.Sprintf("test_%d", i))
	}
}

func TestConsumer(t *testing.T) {
	conn, _, err := zk.Connect([]string{"127.0.0.1:2181", "127.0.0.1:2182", "127.0.0.1:2183"}, time.Second*15)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	queue, err := NewQueue(conn, "/test/queue")
	if err != nil {
		t.Fatal(err)
		return
	}

	go func() {
		for {
			val, err := queue.Consumer()
			if err != nil {
				t.Log(err)
				return
			}
			fmt.Printf("consumer receive val:%s\n", val)
		}
	}()
	var ch chan int
	ch <- 1
}
