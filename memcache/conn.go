package memcache

import (
	"bufio"
	"net"
	"time"
)

type CloseReason uint8

const (
	CloseReasonUnknown = iota
	CloseReasonIdleOverflow
	CloseReasonMemcachedError
)

type OpenConnectionHook func(net.Addr, error)
type CloseConnectionHook func(net.Addr, CloseReason, error)

type Dialer interface {
	Dial(addr net.Addr, timeout time.Duration) (Conn, error)
}

type dialer struct{}

func (d dialer) Dial(addr net.Addr, timeout time.Duration) (Conn, error) {
	nc, err := net.DialTimeout(addr.Network(), addr.String(), timeout)
	if err == nil {
		return &connV2{
			nc:   nc,
			addr: addr,
			rw:   bufio.NewReadWriter(bufio.NewReader(nc), bufio.NewWriter(nc)),
		}, nil
	}

	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return nil, &ConnectTimeoutError{addr}
	}

	return nil, err
}

type Conn interface {
	Addr() net.Addr
	RW() *bufio.ReadWriter
	NetConn() net.Conn
	SetTimeout(time.Duration)
	SetPool(p *Pool)
	PutConn()
	Release(reason CloseReason, err error)
	Close() error
}

type connV2 struct {
	nc   net.Conn
	rw   *bufio.ReadWriter
	addr net.Addr
	p    *Pool
}

func (c *connV2) Close() error {
	return c.nc.Close()
}

func (c connV2) Addr() net.Addr {
	return c.addr
}

//TODO Refactor this shit
func (c *connV2) Release(reason CloseReason, err error) {
	c.p.CloseConn(c, reason, err)
}

func (c *connV2) RW() *bufio.ReadWriter {
	return c.rw
}

func (c *connV2) SetTimeout(timeout time.Duration) {
	c.nc.SetDeadline(time.Now().Add(timeout))
}

func (c *connV2) SetPool(p *Pool) {
	c.p = p
}

func (c *connV2) PutConn() {
	c.p.PutConn(c)
}

func (c *connV2) NetConn() net.Conn {
	return c.nc
}
