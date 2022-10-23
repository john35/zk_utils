package main

import (
	"fmt"
	"github.com/go-zookeeper/zk"
	"time"
)

func main1() {
	conn, _, err := zk.Connect([]string{"127.0.0.1:2181", "127.0.0.1:2182", "127.0.0.1:2183"}, time.Second*15)
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	serviceName := "/cbu"
	data,p,err:=conn.Get("/cbu0000000003")
	if err!=nil{
		panic(err)
	}
	fmt.Println(string(data))
	fmt.Printf("%+v  err:%s\n",p,err)
	fmt.Println(conn.Delete("/cbu0000000003",p.Version))
	str, err := conn.Create(serviceName,[]byte("service.zoomex.notification"),2, zk.WorldACL(zk.PermAll))
	fmt.Println(str,err)
	path, err := conn.CreateProtectedEphemeralSequential(serviceName, []byte("10.17.128.31:9091"), zk.WorldACL(zk.PermAll))
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(path)
	time.Sleep(time.Second * 3)
	children, stat, err := conn.Children(serviceName)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("%+v\n", stat)
	for _, v := range children {
		data, stat, err := conn.Get(v)
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("%+v\n", stat)
		fmt.Println(string(data))
	}
}
