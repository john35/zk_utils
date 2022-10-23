package main

import "github.com/go-zookeeper/zk"

type Barrier struct {
	conn *zk.Conn
	root string
	size int32
}

func NewBarrier(conn *zk.Conn, root string, size int32) (*Barrier, error) {
	isExist, _, err := conn.Exists(root)
	if err != nil {
		return nil, err
	}
	if !isExist {
		if err := createParentPath(conn, root); err != nil {
			return nil, err
		}
	}

	return &Barrier{conn: conn, root: root, size: size}, nil
}

func (b *Barrier) Enter(name string) (bool, error) {
	_, err := b.conn.Create(b.root+"/"+name, []byte{}, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		return false, err
	}
	for {
		paths, _, ch, err := b.conn.ChildrenW(b.root)
		if err != nil {
			return false, err
		}
		if len(paths) >= int(b.size) {
			return true, nil
		}
		<-ch
	}
}

func (b *Barrier) Leave(name string) (bool, error) {
	err := b.conn.Delete(b.root+"/"+name, 0)
	if err != nil {
		return false, err
	}
	for {
		paths, _, ch, err := b.conn.ChildrenW(b.root)
		if err != nil {
			return false, err
		}
		if len(paths) == 0 {
			return true, nil
		}
		<-ch
	}
}
