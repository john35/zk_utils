package main

import (
	"errors"
	"fmt"
	"github.com/go-zookeeper/zk"
	"strconv"
	"strings"
)

var (
	ErrHasLocked = errors.New("has been locked by other process")
)

type DLock struct {
	conn    *zk.Conn
	path    string
	root    string
	version int32
}

func (l DLock) Close() {
	l.conn.Delete(l.root+"/"+l.path, l.version)
}

//GetBlockLock 获取同步阻塞锁
func GetBlockLock(conn *zk.Conn, lockPath string) (*DLock, error) {
	path, err := createEphemeralPath(conn, lockPath)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	fmt.Println(path)
	currentSeq := getPathSeq(path)
	for {
		paths, stat, err := conn.Children(lockPath)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		min := currentSeq
		preSeq := -1
		prePath := ""
		for i, v := range paths {
			fmt.Printf("children :%d path:%s\n", i, v)
			seq := getPathSeq(v)
			if seq < min {
				min = seq
			}
			if seq < currentSeq && seq > preSeq {
				preSeq = seq
				prePath = v
			}
		}
		if min == currentSeq {
			break //success to get lock
		}

		_, stat, ch, err := conn.GetW(lockPath + "/" + prePath)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		fmt.Printf("%+v\n", stat)
		e := <-ch
		fmt.Printf("receive preNode event:%+v\n", e)
	}

	return &DLock{conn: conn, path: path, version: 0, root: lockPath}, nil
}

//GetNoBlockLock 获取非阻塞锁
func GetNoBlockLock(conn *zk.Conn, lockPath string) (*DLock, error) {
	path, err := createEphemeralPath(conn, lockPath)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	fmt.Println(path)
	currentSeq := getPathSeq(path)
	for {
		paths, _, err := conn.Children(lockPath)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		min := currentSeq
		for i, v := range paths {
			fmt.Printf("children :%d path:%s\n", i, v)
			seq := getPathSeq(v)
			if seq < min {
				min = seq
			}
		}
		if min == currentSeq {
			break //success to get lock
		}
		if min < currentSeq {
			return nil, ErrHasLocked
		}
	}

	return &DLock{conn: conn, path: path, version: 0, root: lockPath}, nil
}

func createEphemeralPath(conn *zk.Conn, lockPath string) (string, error) {
	lockPath = strings.TrimSuffix(lockPath, "/")
	//创建临时节点
	prefix := lockPath + "/clock-"
	var path string
	var err error
	for i := 0; i < 3; i++ {
		path, err = conn.CreateProtectedEphemeralSequential(prefix, []byte("my lock"), zk.WorldACL(zk.PermAll))
		if err == zk.ErrNoNode {
			parts := strings.Split(lockPath, "/")
			pPath := ""
			for _, v := range parts[1:] {
				pPath += "/" + v
				isExist, _, err := conn.Exists(pPath)
				if err != nil {
					return "", err
				}
				if isExist {
					continue
				}
				_, err = conn.Create(pPath, nil, 0, zk.WorldACL(zk.PermAll))
				if err != nil {
					return "", err
				}
			}
		}
		if err == nil {
			return path, nil
		}
	}
	return path, err
}

func getPathSeq(path string) int {
	arr := strings.Split(path, "-")
	if len(arr) < 2 {
		return 0
	}
	number := arr[len(arr)-1]
	n, err := strconv.ParseInt(number, 10, 64)
	if err != nil {
		fmt.Println(err)
	}
	return int(n)
}

func createParentPath(conn *zk.Conn, path string) error {
	parts := strings.Split(path, "/")
	pPath := ""
	for _, v := range parts[1:] {
		pPath += "/" + v
		isExist, _, err := conn.Exists(pPath)
		if err != nil {
			return err
		}
		if isExist {
			continue
		}
		_, err = conn.Create(pPath, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
	}
	return nil
}
