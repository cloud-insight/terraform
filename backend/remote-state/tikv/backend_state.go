package tikv

import (
	"context"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform/backend"
	"github.com/hashicorp/terraform/state"
	"github.com/hashicorp/terraform/state/remote"
	"github.com/hashicorp/terraform/states"
	"github.com/hashicorp/terraform/states/statemgr"
)

const (
	keyEnvPrefix = "-env:"
	maxWorkspaces = 10000
)

func (b *Backend) Workspaces() ([]string, error) {
	// List our raw path
	prefix := b.data.Get("path").(string) + keyEnvPrefix
	endKey := append([]byte(prefix), byte(127))
	keys, _, err := b.rawKvClient.Scan(context.TODO(), []byte(prefix), endKey, maxWorkspaces)
	if err != nil {
		return nil, err
	}

	// Find the envs, we use a map since we can get duplicates with
	// path suffixes.
	envs := map[string]struct{}{}
	for _, keyBytes := range keys {
		key := string(keyBytes)
		// Consul should ensure this but it doesn't hurt to check again
		if strings.HasPrefix(key, prefix) {
			key = strings.TrimPrefix(key, prefix)

			// Ignore anything with a "/" in it since we store the state
			// directly in a key not a directory.
			if idx := strings.IndexRune(key, '/'); idx >= 0 {
				continue
			}

			envs[key] = struct{}{}
		}
	}

	result := make([]string, 1, len(envs)+1)
	result[0] = backend.DefaultStateName
	for k := range envs {
		result = append(result, k)
	}

	return result, nil
}

func (b *Backend) DeleteWorkspace(name string) error {
	if name == backend.DefaultStateName || name == "" {
		return fmt.Errorf("can't delete default state")
	}

	// Determine the path of the data
	path := b.path(name)

	// Delete it. We just delete it without any locking since
	// the DeleteState API is documented as such.
	err := b.rawKvClient.Delete(context.TODO(), []byte(path))
	return err
}

func (b *Backend) StateMgr(name string) (statemgr.Full, error) {
	// Determine the path of the data
	path := b.path(name)

	// Build the state client
	var stateMgr = &remote.State{
		Client: &RemoteClient{
			rawKvClient: b.rawKvClient,
			txnKvClient: b.txnKvClient,
			Key:         path,
			DoLock:      b.lock,
		},
	}

	if !b.lock {
		stateMgr.DisableLocks()
	}

	// the default state always exists
	if name == backend.DefaultStateName {
		return stateMgr, nil
	}

	// Grab a lock, we use this to write an empty state if one doesn't
	// exist already. We have to write an empty state as a sentinel value
	// so States() knows it exists.
	lockInfo := state.NewLockInfo()
	lockInfo.Operation = "init"
	lockId, err := stateMgr.Lock(lockInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to lock state in Consul: %s", err)
	}

	// Local helper function so we can call it multiple places
	lockUnlock := func(parent error) error {
		if err := stateMgr.Unlock(lockId); err != nil {
			return fmt.Errorf(strings.TrimSpace(errStateUnlock), lockId, err)
		}

		return parent
	}

	// Grab the value
	if err := stateMgr.RefreshState(); err != nil {
		err = lockUnlock(err)
		return nil, err
	}

	// If we have no state, we have to create an empty state
	if v := stateMgr.State(); v == nil {
		if err := stateMgr.WriteState(states.NewState()); err != nil {
			err = lockUnlock(err)
			return nil, err
		}
		if err := stateMgr.PersistState(); err != nil {
			err = lockUnlock(err)
			return nil, err
		}
	}

	// Unlock, the state should now be initialized
	if err := lockUnlock(nil); err != nil {
		return nil, err
	}

	return stateMgr, nil
}

func (b *Backend) path(name string) string {
	path := b.data.Get("path").(string)
	if name != backend.DefaultStateName {
		path += fmt.Sprintf("%s%s", keyEnvPrefix, name)
	}

	return path
}

const errStateUnlock = `
Error unlocking TiKV state. Lock ID: %s

Error: %s

You may have to force-unlock this state in order to use it again.
The TiKV backend acquires a lock during initialization to ensure
the minimum required key/values are prepared.
`
