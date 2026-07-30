package registry

import (
	"context"
	"errors"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type Etcd struct {
	client *clientv3.Client
}

func NewEtcd(client *clientv3.Client) *Etcd {
	return &Etcd{
		client: client,
	}
}

func (e *Etcd) Register(ctx context.Context, key, node string) (IRegistration, error) {
	if e.client == nil {
		return nil, errors.New("etcd client is nil")
	}
	if key == "" {
		return nil, errors.New("actor ID is empty")
	}
	if node == "" {
		return nil, errors.New("node address is empty")
	}

	session, err := concurrency.NewSession(e.client, concurrency.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("create actor session: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := e.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", 0)).
		Then(clientv3.OpPut(key, node, clientv3.WithLease(session.Lease()))).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		_ = session.Close()
		return nil, fmt.Errorf("register actor transaction: %w", err)
	}
	if resp.Succeeded {
		registration := &EtcdRegistration{
			session: session,
		}
		return registration, nil
	}

	_ = session.Close()
	if len(resp.Responses) == 0 {
		return nil, errors.New("actor registration conflict returned no response")
	}
	getResp := resp.Responses[0].GetResponseRange()
	if getResp == nil || len(getResp.Kvs) == 0 {
		return nil, errors.New("actor registration conflict returned no owner")
	}
	return nil, fmt.Errorf("actor registration conflict: %s", string(getResp.Kvs[0].Value))
}

func (e *Etcd) Get(ctx context.Context, key string) (string, error) {
	get, err := e.client.Get(ctx, key)
	if err != nil {
		return "", err
	}
	if len(get.Kvs) == 0 {
		return "", errors.New("actor registration not found")
	}
	return string(get.Kvs[0].Value), nil
}

type EtcdRegistration struct {
	session *concurrency.Session
}

func (e *EtcdRegistration) Done() <-chan struct{} {
	return e.session.Done()
}

func (e *EtcdRegistration) Close() error {
	if err := e.session.Close(); err != nil {
		return fmt.Errorf("close actor session: %w", err)
	}
	return nil
}
