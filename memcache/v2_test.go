package memcache

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient_getPool(t *testing.T) {
	addr := &staticAddr{ntw: "tcp", str: "testAddr1"}
	t.Run("pool is exist, get existed pool", func(t *testing.T) {
		c := &Client{
			pools: newTestPoolWithAddr(addr),
		}
		p, err := c.getPool(addr)

		require.NoError(t, err)
		assert.Equal(t, p.addr, addr)
	})
	t.Run("pool is not exist, create new pool, get created pool", func(t *testing.T) {
		c := &Client{
			pools: make(map[string]*Pool),
		}
		p, err := c.getPool(addr)

		require.NoError(t, err)
		assert.Equal(t, p.addr, addr)
	})
}

func newTestPoolWithAddr(addr net.Addr) map[string]*Pool {
	return map[string]*Pool{
		addr.String(): &Pool{
			addr: addr,
		},
	}
}
