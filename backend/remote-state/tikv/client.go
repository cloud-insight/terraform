package tikv

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	_ "github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rawkv"
	"github.com/tikv/client-go/txnkv"
	"github.com/hashicorp/terraform/state"
	"github.com/hashicorp/terraform/state/remote"
)

const (
	lockAcquireTimeout = 2 * time.Second
	lockInfoSuffix     = ".lockinfo"
)

// RemoteClient is a remote client that will store data in etcd.
type RemoteClient struct {
	DoLock bool
	Key    string

	rawKvClient *rawkv.Client
	txnKvClient *txnkv.Client
	info        *state.LockInfo
	mu          sync.Mutex
}

func (c *RemoteClient) Get() (*remote.Payload, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	res, err := c.rawKvClient.Get(context.TODO(), []byte(c.Key))
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}

	payload := res
	md5 := md5.Sum(payload)

	return &remote.Payload{
		Data: payload,
		MD5:  md5[:],
	}, nil
}

func (c *RemoteClient) Put(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	err := c.rawKvClient.Put(context.TODO(),[]byte(c.Key), []byte(data))
	if err != nil {
		return err
	}

	return nil
}

func (c *RemoteClient) Delete() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	err := c.rawKvClient.Delete(context.TODO(), []byte(c.Key))
	return err
}

func (c *RemoteClient) Lock(info *state.LockInfo) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.DoLock {
		return "", nil
	}

	c.info = info
	return c.lock()
}

func (c *RemoteClient) Unlock(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.DoLock {
		return nil
	}

	return c.unlock(id)
}

func (c *RemoteClient) deleteLockInfo(info *state.LockInfo) error {
	// TODO check delete not existed item
	err := c.rawKvClient.Delete(context.TODO(), []byte(c.Key+lockInfoSuffix))
	if err != nil {
		return err
	}
	/*
	if res.Deleted == 0 {
		return fmt.Errorf("No keys deleted for %s when deleting lock info.", c.Key+lockInfoSuffix)
	}
	 */
	return nil
}

func (c *RemoteClient) getLockInfo(tx *txnkv.Transaction) (*state.LockInfo, error) {
	res, err := c.rawKvClient.Get(context.TODO(), []byte(c.Key+lockInfoSuffix))
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}

	li := &state.LockInfo{}
	err = json.Unmarshal(res, li)
	if err != nil {
		return nil, fmt.Errorf("Error unmarshaling lock info: %s.", err)
	}

	return li, nil
}

func (c *RemoteClient) putLockInfo(tx *txnkv.Transaction, info *state.LockInfo) error {
	//c.info.Path = c.etcdMutex.Key()
	c.info.Created = time.Now().UTC()
	err := tx.Set([]byte(c.Key+lockInfoSuffix), []byte(string(c.info.Marshal())))
	return err
}

func (c *RemoteClient) lock() (string, error) {
	tx, err := c.txnKvClient.Begin(context.TODO())
	if err != nil {
		return "", err
	}
	resp, err := tx.Get(context.TODO(), []byte(c.Key+lockInfoSuffix))
	if err != nil {
		return "", &state.LockError{Err: err}
	}
	if resp != nil {
		lockInfo, err := c.getLockInfo(tx)
		if err != nil {
			return "", &state.LockError{Err: err}
		}
		return "", &state.LockError{Info: lockInfo, Err: errors.New("lock is conflict")}
	}
	err = c.putLockInfo(tx, c.info)
	if err != nil {
		return "", &state.LockError{Err: err}
	}
	tx.Commit(context.TODO())
	return c.info.ID, nil
}

func (c *RemoteClient) unlock(id string) error {
	return c.deleteLockInfo(c.info)
}

