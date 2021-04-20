package memcache

import (
	"context"
	"errors"
	"net"
)

func InitConnectionPools(ss ServerSelector, config Config) (map[string]*Pool, error) {
	pools := make(map[string]*Pool)

	err := ss.Each(func(addr net.Addr) error {
		p, err := NewPoolWithConfig(addr, config)
		if err != nil {
			return err
		}
		err = p.InitConnections()
		if err != nil {
			return err
		}
		pools[addr.String()] = p
		return nil
	})
	if err != nil {
		return nil, err
	}
	return pools, nil
}

func NewPool(addr net.Addr) (*Pool, error) {
	return NewPoolWithConfig(addr, DefaultConfig)
}

func NewPoolWithConfig(addr net.Addr, config Config) (*Pool, error) {
	p := &Pool{
		config: config,
		addr:   addr,
		dialer: dialer{},
	}

	p.connections = make(chan Conn, p.config.MaxIdleConnections)

	if p.config.MaxOpenConnections > 0 {
		p.maxOpenConnectionsCh = make(chan struct{}, p.config.MaxOpenConnections)
	}

	return p, nil
}

type Pool struct {
	addr                 net.Addr
	connections          chan Conn
	maxOpenConnectionsCh chan struct{}

	config Config

	dialer Dialer
}

func (p *Pool) InitConnections() error {
	for i := 0; i < p.config.InitConnections; i++ {
		cn, err := p.newConn()
		if err != nil {
			return err
		}
		p.connections <- cn
	}
	return nil
}

func (p *Pool) reserveConnection() bool {
	if p.maxOpenConnectionsCh == nil {
		return true
	}
	select {
	case p.maxOpenConnectionsCh <- struct{}{}:
		return true
	default:
		return false
	}
}

func (p *Pool) freeConnection() {
	if p.maxOpenConnectionsCh == nil {
		return
	}
	<-p.maxOpenConnectionsCh
}

func (p *Pool) GetConn(ctx context.Context) (Conn, error) {
	select {
	case cn := <-p.connections:
		return cn, nil
	default:
	}

	go func() {
		cn, err := p.newConn()
		if err != nil {
			return
		}
		p.PutConn(cn)
	}()

	select {
	case cn := <-p.connections:
		return cn, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *Pool) PutConn(cn Conn) () {
	select {
	case p.connections <- cn:
	default:
		p.CloseConn(cn)
	}
}

func (p *Pool) CloseConn(cn Conn) {
	cn.Close()
	if p.config.CloseConnectionHook != nil {
		p.config.CloseConnectionHook(p.addr, nil)
	}
	p.freeConnection()
}

func (p *Pool) newConn() (Conn, error) {
	reserved := p.reserveConnection()
	if !reserved {
		return nil, errors.New("exceeded limit of max connections")
	}

	cn, err := p.dialer.Dial(p.addr, p.config.ConnectTimeout)

	if p.config.OpenConnectionHook != nil {
		p.config.OpenConnectionHook(p.addr, err)
	}

	if err != nil {
		p.freeConnection()
		return nil, err
	}
	cn.SetPool(p)

	return cn, nil
}
