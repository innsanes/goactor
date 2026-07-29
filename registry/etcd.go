package registry

import (
	"context"
	"errors"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type Etcd struct {
	session *concurrency.Session
	client  *clientv3.Client
}

func NewEtcd(client *clientv3.Client) *Etcd {
	return &Etcd{
		client: client,
	}
}

func (e *Etcd) Register(ctx context.Context, key, node string, ttl int) error {
	if e.client == nil {
		return errors.New("etcd client is nil")
	}
	if key == "" {
		return errors.New("actor ID is empty")
	}
	if node == "" {
		return errors.New("node address is empty")
	}
	if ttl <= 0 {
		return errors.New("session TTL must be positive")
	}

	session, err := concurrency.NewSession(
		e.client,
		concurrency.WithContext(ctx),
		concurrency.WithTTL(ttl),
	)
	if err != nil {
		return fmt.Errorf("create actor session: %w", err)
	}
	e.session = session

	resp, err := e.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", 0)).
		Then(clientv3.OpPut(key, node, clientv3.WithLease(e.session.Lease()))).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		_ = e.session.Close()
		return fmt.Errorf("register actor transaction: %w", err)
	}
	if resp.Succeeded {
		return nil
	}

	_ = e.session.Close()
	if len(resp.Responses) == 0 {
		return errors.New("actor registration conflict returned no response")
	}
	getResp := resp.Responses[0].GetResponseRange()
	if getResp == nil || len(getResp.Kvs) == 0 {
		return errors.New("actor registration conflict returned no owner")
	}
	return fmt.Errorf("actor registration conflict: %s", string(getResp.Kvs[0].Value))
}

func (e *Etcd) Done() <-chan struct{} {
	return e.session.Done()
}

func (e *Etcd) Close() error {
	if err := e.session.Close(); err != nil {
		return fmt.Errorf("close actor session: %w", err)
	}
	return nil
}
