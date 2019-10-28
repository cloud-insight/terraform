package tikv

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/tikv/client-go/key"

	"github.com/hashicorp/terraform/backend"
	"github.com/hashicorp/terraform/state"
	"github.com/hashicorp/terraform/state/remote"
)

func TestRemoteClient_impl(t *testing.T) {
	var _ remote.Client = new(RemoteClient)
}

func TestRemoteClient(t *testing.T) {
	prepareTiKV(t)
	defer cleanupTiKV(t)

	prefix := fmt.Sprintf("%s/%s/", keyPrefix, time.Now().Format(time.RFC3339))

	// Get the backend
	b := backend.TestBackendConfig(t, New(), backend.TestWrapConfig(map[string]interface{}{
		"pd_addresses": tikvAddressesCty,
		"prefix":     prefix,
	}))

	// Grab the client
	s, err := b.StateMgr(backend.DefaultStateName)
	if err != nil {
		t.Fatalf("Error: %s.", err)
	}

	// Test
	remote.TestClient(t, s.(*remote.State).Client)
}

func TestTiKV_stateLock(t *testing.T) {
	prepareTiKV(t)
	defer cleanupTiKV(t)

	prefix := fmt.Sprintf("%s/%s/", keyPrefix, time.Now().Format(time.RFC3339))

	// Get the backend
	s1, err := backend.TestBackendConfig(t, New(), backend.TestWrapConfig(map[string]interface{}{
		"pd_addresses": tikvAddressesCty,
		"prefix":     prefix,
	})).StateMgr(backend.DefaultStateName)
	if err != nil {
		t.Fatal(err)
	}

	s2, err := backend.TestBackendConfig(t, New(), backend.TestWrapConfig(map[string]interface{}{
		"pd_addresses": tikvAddressesCty,
		"prefix":     prefix,
	})).StateMgr(backend.DefaultStateName)
	if err != nil {
		t.Fatal(err)
	}

	remote.TestRemoteLocks(t, s1.(*remote.State).Client, s2.(*remote.State).Client)
}

func TestTiKV_destroyLock(t *testing.T) {
	prepareTiKV(t)
	defer cleanupTiKV(t)

	prefix := fmt.Sprintf("%s/%s/", keyPrefix, time.Now().Format(time.RFC3339))

	// Get the backend
	b := backend.TestBackendConfig(t, New(), backend.TestWrapConfig(map[string]interface{}{
		"pd_addresses": tikvAddressesCty,
		"prefix":     prefix,
	}))

	// Grab the client
	s, err := b.StateMgr(backend.DefaultStateName)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	c := s.(*remote.State).Client.(*RemoteClient)

	info := state.NewLockInfo()
	id, err := c.Lock(info)
	if err != nil {
		t.Fatal(err)
	}

	if err := c.Unlock(id); err != nil {
		t.Fatal(err)
	}

	ctx := context.TODO()
	txn, err := c.txnKvClient.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}

	res, err := txn.Get(ctx, key.Key(c.info.Path))
	if err == nil {
		if e := txn.Commit(ctx); e != nil {
			err = multierror.Append(err, e)
		}
	}
	if err != nil {
		t.Fatal(err)
	}

	if res != nil {
		t.Fatalf("lock key not cleaned up at: %s", c.info.Path)
	}
}
