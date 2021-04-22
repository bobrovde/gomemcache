package memcache

import (
	"bufio"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockDialer struct {
	dialFn func(addr net.Addr, timeout time.Duration) (Conn, error)
}

func (m mockDialer) Dial(addr net.Addr, timeout time.Duration) (Conn, error) {
	return m.dialFn(addr, timeout)
}

func TestPool_GetConn(t *testing.T) {
	t.Run("no connections in pool, dail new one without error", func(t *testing.T) {
		p := newPool(t)

		conn, err := p.GetConn(context.Background())

		require.NoError(t, err)
		assert.Equal(t, "testAddr", conn.Addr().String())
	})
	t.Run("no connections in pool, failed to dail new one, got error", func(t *testing.T) {
		p := newPool(t)
		p.dialer = mockDialer{func(addr net.Addr, timeout time.Duration) (Conn, error) {
			return nil, errors.New("some error")
		}}
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
		defer cancel()

		conn, err := p.GetConn(ctx)

		assert.Error(t, err)
		assert.Nil(t, conn)
	})
	t.Run("there is connection in pool, get exist connection", func(t *testing.T) {
		p := newPool(t)
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
		defer cancel()

		cn, err := p.newConn()
		require.NoError(t, err)
		p.PutConn(cn)
		p.dialer = withMockDialerReturn(nil, errors.New("some error"))
		conn, err := p.GetConn(ctx)

		require.NoError(t, err)
		assert.Equal(t, connAddr, conn.Addr().String())
	})
	t.Run("no connections in pool, start to dial, get returned connection", func(t *testing.T) {
		p := newPool(t)

		cn, err := p.newConn()
		require.NoError(t, err)
		p.dialer = mockDialer{dialFn: func(addr net.Addr, timeout time.Duration) (Conn, error) {
			p.PutConn(cn)
			time.Sleep(time.Second * 1)
			return nil, errors.New("some error")
		}}
		conn, err := p.GetConn(context.Background())

		require.NoError(t, err)
		assert.Equal(t, connAddr, conn.Addr().String())
	})
	t.Run("no connections in pool, start to dial with canceled context, get error", func(t *testing.T) {
		p := newPool(t)
		ctx, cancel := context.WithCancel(context.Background())

		p.dialer = mockDialer{dialFn: func(addr net.Addr, timeout time.Duration) (Conn, error) {
			cancel()
			time.Sleep(time.Second * 1)
			return &connV2{addr: addr}, nil
		}}
		conn, err := p.GetConn(ctx)

		assert.Error(t, err)
		assert.Nil(t, conn)
	})
}

func TestPool_newConn(t *testing.T) {
	newPoolForNewConn := func() *Pool {
		cfg := DefaultConfig
		cfg.MaxOpenConnections = 1
		return newPoolWithConfig(t, cfg)
	}
	t.Run("max connections limit not exceeded, got connection", func(t *testing.T) {
		p := newPoolForNewConn()

		cn, err := p.newConn()

		require.NoError(t, err)
		assert.Equal(t, connAddr, cn.Addr().String())
	})
	t.Run("max connections limit is exceeded, got error", func(t *testing.T) {
		p := newPoolForNewConn()

		_, err := p.newConn()
		require.NoError(t, err)
		cn, err := p.newConn()

		require.Error(t, err)
		assert.Nil(t, cn)
	})
	t.Run("max connections limit is not exceeded, success dial after failed dial, got connection", func(t *testing.T) {
		p := newPoolForNewConn()

		p.dialer = withMockDialerReturn(nil, errors.New("some error"))
		_, err := p.newConn()
		assert.Error(t, err)
		p.dialer = withMockDialerReturn(&connV2{addr: &staticAddr{"tcp", connAddr}}, nil)
		cn, err := p.newConn()

		require.NoError(t, err)
		assert.Equal(t, connAddr, cn.Addr().String())
	})
	t.Run("OpenConnectionHook is not nil, expect hook was called", func(t *testing.T) {
		p := newPoolForNewConn()
		var openConnectionHookCalled int
		p.config.OpenConnectionHook = func(addr net.Addr, err error) {
			openConnectionHookCalled++
			assert.Equal(t, connAddr, addr.String())
			assert.NoError(t, err)
		}

		p.newConn()

		assert.Equal(t, 1, openConnectionHookCalled)
	})
}

func TestPool_PutConn(t *testing.T) {
	newPoolForPutConn := func() *Pool {
		cfg := DefaultConfig
		cfg.MaxIdleConnections = 1
		cfg.MaxOpenConnections = 2
		cfg.InitConnections = 1
		return newPoolWithConfig(t, cfg)
	}
	t.Run("idle connections channel is empty, expect put connection to pool", func(t *testing.T) {
		p := newPoolForPutConn()
		var closeConnCalled int
		p.dialer = mockDialer{func(addr net.Addr, timeout time.Duration) (Conn, error) {
			return mockConn{func() error {
				closeConnCalled++
				return nil
			}}, nil
		}}

		dialedConnection, err := p.GetConn(context.Background())
		require.NoError(t, err)
		p.PutConn(dialedConnection)

		assert.EqualValues(t, 0, closeConnCalled)
		assert.Len(t, p.connections, 1)
	})
	t.Run("idle connections channel is full, expect close connection", func(t *testing.T) {
		p := newPoolForPutConn()
		var closeConnCalled int
		p.dialer = mockDialer{func(addr net.Addr, timeout time.Duration) (Conn, error) {
			return mockConn{func() error {
				closeConnCalled++
				return nil
			}}, nil
		}}

		err := p.InitConnections()
		require.NoError(t, err)
		connectionFromPool, err := p.GetConn(context.Background())
		require.NoError(t, err)
		dialedConnection, err := p.GetConn(context.Background())
		require.NoError(t, err)

		p.PutConn(connectionFromPool)
		assert.EqualValues(t, 0, closeConnCalled)
		p.PutConn(dialedConnection)
		assert.EqualValues(t, 1, closeConnCalled)
	})
}

func TestHooks(t *testing.T) {
	t.Run("OpenConnectionHook is not nil, expect hook was called", func(t *testing.T) {
		p := newPool(t)
		var openConnectionHookCalled int
		p.config.OpenConnectionHook = func(addr net.Addr, err error) {
			openConnectionHookCalled++
			assert.Equal(t, connAddr, addr.String())
			assert.NoError(t, err)
		}

		p.newConn()

		assert.Equal(t, 1, openConnectionHookCalled)
	})
	t.Run("CloseConnectionHook is not nil, expect hook was called", func(t *testing.T) {
		p := newPool(t)
		p.dialer = mockDialer{func(addr net.Addr, timeout time.Duration) (Conn, error) {
			return mockConn{func() error {
				return nil
			}}, nil
		}}
		var closeConnectionHookCalled int
		p.config.CloseConnectionHook = func(addr net.Addr, err error) {
			closeConnectionHookCalled++
			assert.Equal(t, connAddr, addr.String())
			assert.NoError(t, err)
		}

		cn, err := p.GetConn(context.Background())
		require.NoError(t, err)
		p.CloseConn(cn)

		assert.Equal(t, 1, closeConnectionHookCalled)
	})
}

func TestPool_InitConnections(t *testing.T) {
	t.Run("dialer without error, expect success init", func(t *testing.T) {
		p := newPool(t)

		err := p.InitConnections()

		assert.NoError(t, err)
		assert.Len(t, p.connections, p.config.InitConnections)
	})
	t.Run("dialer with error, expect failed init", func(t *testing.T) {
		p := newPool(t)
		p.dialer = withMockDialerReturn(nil, errors.New("some error"))

		err := p.InitConnections()

		assert.Error(t, err)
		assert.Len(t, p.connections, 0)
	})
}

const connAddr = "testAddr"

func newPool(t *testing.T) *Pool {
	return newPoolWithConfig(t, DefaultConfig)
}

func newPoolWithConfig(t *testing.T, cfg Config) *Pool {
	p, err := NewPoolWithConfig(&staticAddr{"tcp", connAddr}, cfg)
	require.NoError(t, err)
	p.dialer = mockDialer{func(addr net.Addr, timeout time.Duration) (Conn, error) {
		return &connV2{addr: addr}, nil
	}}

	return p
}

func withMockDialerReturn(cn Conn, err error) mockDialer {
	return mockDialer{func(addr net.Addr, timeout time.Duration) (Conn, error) {
		return cn, err
	}}
}

type mockConn struct {
	closeFn func() error
}

func (m mockConn) Close() error {
	return m.closeFn()
}

func (m mockConn) RW() *bufio.ReadWriter {
	return nil
}

func (m mockConn) Addr() net.Addr {
	return nil
}

func (m mockConn) SetTimeout(duration time.Duration) {
	return
}

func (m mockConn) SetPool(p *Pool) {
	return
}

func (m mockConn) Release() {
	return
}
