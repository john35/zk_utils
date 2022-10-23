package zk_utils

import (
	"fmt"
	"github.com/go-zookeeper/zk"
	"sync"
	"testing"
	"time"
)

func Test_getPathSeq(t *testing.T) {
	path := "_c_fbe1f45e14a4e4c272a889c680906869-clock-0000000003"
	if got := getPathSeq(path); got != 3 {
		t.Errorf("getPathSeq() = %v, want %v", got, 3)
	}
}

func Test_getLock(t *testing.T) {
	conn, _, err := zk.Connect([]string{"127.0.0.1:2181", "127.0.0.1:2182", "127.0.0.1:2183"}, time.Second*15)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	lock, err := GetBlockLock(conn, "/nie/john/hi/hello")
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
}

func do(i int) {
	conn, _, err := zk.Connect([]string{"127.0.0.1:2181", "127.0.0.1:2182", "127.0.0.1:2183"}, time.Second*15)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	lock, err := GetBlockLock(conn, "/dlock")
	if err != nil {
		fmt.Printf("node:%d get lock err:%s", i, err)
	}
	defer lock.Close()
	fmt.Printf("node:%d is processing business\n", i)
	time.Sleep(time.Second * 3)
	fmt.Printf("node:%d has done\n", i)
}

func Test_Lock(t *testing.T) {
	wait := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wait.Add(1)
		go func(i int) {
			defer wait.Done()
			do(i)
		}(i)
	}
	wait.Wait()
}
