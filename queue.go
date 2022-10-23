package main

import (
	"fmt"
	"github.com/go-zookeeper/zk"
	"strconv"
	"strings"
)

/*
 Producer-Consumer Queues implementation by zk
*/

type Queue struct {
	conn *zk.Conn
	root string
}

func NewQueue(conn *zk.Conn, root string) (*Queue, error) {
	isExist, _, err := conn.Exists(root)
	if err != nil {
		return nil, err
	}
	if !isExist {
		if err := createParentPath(conn, root); err != nil {
			return nil, err
		}
	}
	return &Queue{conn: conn, root: root}, nil
}

func (q *Queue) Producer(val string) error {
	_, err := q.conn.Create(q.root+"/"+"element", []byte(val), zk.FlagSequence, zk.WorldACL(zk.PermAll))
	return err
}

func (q *Queue) Consumer() (string, error) {
	var parseSeq = func(path string) (int64, error) {
		seqStr := strings.TrimPrefix(path, "element")
		seq, err := strconv.ParseInt(seqStr, 10, 64)
		if err != nil {
			fmt.Println(err, path)
			return 0, err
		}
		return seq, nil
	}
	for {
		paths, _, ch, err := q.conn.ChildrenW(q.root)
		if err != nil {
			return "", err
		}
		if len(paths) == 0 {
			<-ch
			continue
		}
		var minPath = paths[0]
		var minSeq int64
		seq, err := parseSeq(minPath)
		if err != nil {
			return "", err
		}
		minSeq = seq

		for _, path := range paths {
			seq, err := parseSeq(minPath)
			if err != nil {
				return "", err
			}
			if seq < minSeq {
				minSeq = seq
				minPath = path
				break
			}
		}
		data, stat, err := q.conn.Get(q.root + "/" + minPath)
		if err != nil {
			return "", err
		}
		if err := q.conn.Delete(q.root+"/"+minPath, stat.Version); err != nil {
			fmt.Println(err)
			return "", err
		}
		return string(data), nil
	}
}
