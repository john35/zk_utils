package zk_utils

import (
	"fmt"
	"github.com/go-zookeeper/zk"
	"sync"
)

/*
	two phase commit
*/

type TwoPhaseCommitCoordinator struct {
	sync.Mutex
	conn             *zk.Conn
	root             string
	seq              int64
	registerServices map[string]struct{}
}

func NewTwoPhaseCommitCoordinator(conn *zk.Conn, root string) (*TwoPhaseCommitCoordinator, error) {
	isExist, _, err := conn.Exists(root)
	if err != nil {
		return nil, err
	}
	if !isExist {
		if err := createParentPath(conn, root); err != nil {
			return nil, err
		}
	}
	return &TwoPhaseCommitCoordinator{conn: conn, root: root}, nil
}

func (t *TwoPhaseCommitCoordinator) RegisterService(serviceNames ...string) {
	t.Lock()
	defer t.Unlock()
	for _, serviceName := range serviceNames {
		t.registerServices[serviceName] = struct{}{}
	}
}

func (t *TwoPhaseCommitCoordinator) Begin2PC() (string, error) {
	t.Lock()
	t.seq++
	t.Unlock()
	txPath := fmt.Sprintf("%s/tx_%d", t.root, t.seq)
	_, err := t.conn.Create(txPath, []byte{}, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		return "", err
	}

	for serviceName := range t.registerServices {
		tmpPath := txPath + "/" + serviceName
		_, err := t.conn.Create(tmpPath, []byte{}, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return "", err
		}
	}
	go func() {
		for {
			subPaths, stat, ch, err := t.conn.ChildrenW(txPath)
			if err != nil {
				return
			}
			versions := map[string]int32{}
			for _, path := range subPaths {
				fullPath := txPath + "/" + path
				data, stat, err := t.conn.Get(fullPath)
				if err != nil {
					fmt.Println(err)
					return
				}
				if string(data) == "" {
					<-ch
					break
				}
				if string(data) != "YES" {
					t.broadcast("ABORT")
					return
				}
				versions[fullPath] = stat.Version
			}
			//commit
			t.broadcast("COMMIT")
		}
	}()
	return txPath, nil
}

func (t *TwoPhaseCommitCoordinator) ListenCommit() (bool, error) {
	txPath := t.txPath()
	for {
		subPaths, _, ch, err := t.conn.ChildrenW(txPath)
		if err != nil {
			return false, err
		}
		versions := map[string]int32{}
		for _, path := range subPaths {
			fullPath := txPath + "/" + path
			data, stat, err := t.conn.Get(fullPath)
			if err != nil {
				fmt.Println(err)
				return false, err
			}
			if string(data) == "" {
				<-ch
				break
			}
			if string(data) != "YES" {
				t.broadcast("ABORT")
				return false, nil
			}
			versions[fullPath] = stat.Version
		}
		//commit
	}
	err := t.broadcast("COMMIT")
	if err != nil {
		return false, err
	}
	return true, nil
}

func (t *TwoPhaseCommitCoordinator) broadcast(msg string) error {
	txPath := t.txPath()
	for service := range t.registerServices {
		tmpPath := txPath + "/" + service
		_, stat, err := t.conn.Get(tmpPath)
		if err != nil {
			fmt.Println(err)
			return err
		}
		if _, err := t.conn.Set(tmpPath, []byte(msg), stat.Version+1); err != nil {
			return err
		}
	}
	return nil
}

func (t *TwoPhaseCommitCoordinator) txPath() string {
	return fmt.Sprintf("%s/tx_%d", t.root, t.seq)
}
