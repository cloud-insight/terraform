package tikv

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rawkv"
	"github.com/tikv/client-go/txnkv"

	"github.com/hashicorp/terraform/backend"
)

var (
	tikvAddresses = strings.Split(os.Getenv("TF_TIKV_PD_ADDRESS"), ",")
)

const (
	keyPrefix = "tf-unit"
)

func TestBackend_impl(t *testing.T) {
	var _ backend.Backend = new(Backend)
}

func cleanupTiKV(t *testing.T) {
	var err error
	ctx := context.TODO()

	cfg := config.Config{}
	rawKvClient, err := rawkv.NewClient(ctx, tikvAddresses, cfg)
	if err != nil {
		t.Fatal(err)
	}

	_, err = txnkv.NewClient(ctx, tikvAddresses, cfg)
	if err != nil {
		t.Fatal(err)
	}

	keyBytes := []byte(keyPrefix)
	err  = rawKvClient.DeleteRange(ctx, keyBytes, append(keyBytes, byte(127)))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Cleaned up all tikv keys.")
}

func prepareTiKV(t *testing.T) {
	skip := os.Getenv("TF_ACC") == "" && os.Getenv("TF_TIKV_TEST") == ""
	if skip {
		t.Log("tikv server tests require setting TF_ACC or TF_TIKV_TEST")
		t.Skip()
	}
	if reflect.DeepEqual(tikvAddresses, []string{""}) {
		t.Fatal("tikv server tests require setting TF_TIKV_PD_ADDRESS")
	}
	cleanupTiKV(t)
}

func TestBackend(t *testing.T) {
	prepareTiKV(t)
	defer cleanupTiKV(t)

	prefix := fmt.Sprintf("%s/%s/", keyPrefix, time.Now().Format(time.RFC3339))

	// Get the backend. We need two to test locking.
	b1 := backend.TestBackendConfig(t, New(), backend.TestWrapConfig(map[string]interface{}{
		"pd_address": tikvAddresses,
		"prefix":     prefix,
	}))

	b2 := backend.TestBackendConfig(t, New(), backend.TestWrapConfig(map[string]interface{}{
		"pd_address": tikvAddresses,
		"prefix":     prefix,
	}))

	// Test
	backend.TestBackendStates(t, b1)
	backend.TestBackendStateLocks(t, b1, b2)
	backend.TestBackendStateForceUnlock(t, b1, b2)
}

func TestBackend_lockDisabled(t *testing.T) {
	prepareTiKV(t)
	defer cleanupTiKV(t)

	prefix := fmt.Sprintf("%s/%s/", keyPrefix, time.Now().Format(time.RFC3339))

	// Get the backend. We need two to test locking.
	b1 := backend.TestBackendConfig(t, New(), backend.TestWrapConfig(map[string]interface{}{
		"pd_address": tikvAddresses,
		"prefix":     prefix,
		"lock":       false,
	}))

	b2 := backend.TestBackendConfig(t, New(), backend.TestWrapConfig(map[string]interface{}{
		"pd_address": tikvAddresses,
		"prefix":     prefix + "/" + "different", // Diff so locking test would fail if it was locking
		"lock":       false,
	}))

	// Test
	backend.TestBackendStateLocks(t, b1, b2)
}
