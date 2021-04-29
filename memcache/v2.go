package memcache

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

//TODO Validate config
func NewV2(ss ServerSelector, config Config) (*Client, error) {
	c := NewFromSelector(ss)
	var err error
	c.pools, err = InitConnectionPools(ss, config)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Client) getConnWithContext(ctx context.Context, addr net.Addr) (Conn, error) {
	p, err := c.getPool(addr)
	if err != nil {
		return nil, err
	}
	return p.GetConn(ctx)
}

func (c *Client) getPool(addr net.Addr) (*Pool, error) {
	c.muPool.RLock()
	pool, ok := c.pools[addr.String()]
	c.muPool.RUnlock()
	if ok {
		return pool, nil
	}

	c.muPool.Lock()
	defer c.muPool.Unlock()

	pool, ok = c.pools[addr.String()]
	if ok {
		return pool, nil
	}

	pool, err := NewPoolWithConfig(addr, c.config)
	if err != nil {
		return nil, err
	}

	c.pools[addr.String()] = pool
	return pool, nil
}

func (c *Client) GetV2(ctx context.Context, key string) (item *Item, err error) {
	err = c.withKeyAddr(key, func(addr net.Addr) error {
		return c.getFromAddrWithCtx(ctx, addr, []string{key}, func(it *Item) { item = it })
	})
	if err == nil && item == nil {
		err = ErrCacheMiss
	}
	return
}

// Delete deletes the item with the provided key. The error ErrCacheMiss is
// returned if the item didn't already exist in the cache.
func (c *Client) DeleteV2(ctx context.Context, key string) error {
	return c.withKeyAddr(key, func(addr net.Addr) error {
		return c.withAddrRwWithCtx(ctx, addr, func(_ net.Conn, rw *bufio.ReadWriter) error {
			return writeExpectf(rw, resultDeleted, "delete %s\r\n", key)
		})
	})
}

// GetMultiByAddr, caller have to be sure that all keys placed on one server
// In method skipped keys validation
func (c *Client) GetMultiByAddrV2(ctx context.Context, addr net.Addr, keys []string) (map[string]*Item, error) {
	m := make(map[string]*Item)
	addItemToMap := func(it *Item) {
		m[it.Key] = it
	}

	err := c.getFromAddrWithCtx(ctx, addr, keys, addItemToMap)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (c *Client) GetMultiV2(ctx context.Context, keys []string) (map[string]*Item, error) {
	var lk sync.Mutex
	m := make(map[string]*Item)
	addItemToMap := func(it *Item) {
		lk.Lock()
		defer lk.Unlock()
		m[it.Key] = it
	}

	keyMap := make(map[net.Addr][]string)
	for _, key := range keys {
		if !legalKey(key) {
			return nil, ErrMalformedKey
		}
		addr, err := c.selector.PickServer(key)
		if err != nil {
			return nil, err
		}
		keyMap[addr] = append(keyMap[addr], key)
	}

	ch := make(chan error, buffered)
	for addr, keys := range keyMap {
		go func(addr net.Addr, keys []string) {
			ch <- c.getFromAddrWithCtx(ctx, addr, keys, addItemToMap)
		}(addr, keys)
	}

	var err error
	for _ = range keyMap {
		if ge := <-ch; ge != nil {
			err = ge
		}
	}
	return m, err
}

func (c *Client) DeleteMultiV2(ctx context.Context, keys []string) error {
	keysMap := make(map[net.Addr][]string)

	for _, key := range keys {
		if !legalKey(key) {
			return ErrMalformedKey
		}

		addr, err := c.selector.PickServer(key)
		if err != nil {
			return err
		}

		keysMap[addr] = append(keysMap[addr], key)
	}

	ch := make(chan error, buffered)
	for addr, keys := range keysMap {
		go func(addr net.Addr, keys []string) {
			ch <- c.deleteManyWithContext(ctx, addr, keys)
		}(addr, keys)
	}

	var responseErr error
	for _ = range keysMap {
		if ge := <-ch; ge != nil {
			responseErr = ge
		}
	}

	return responseErr
}

func (c *Client) deleteManyWithContext(ctx context.Context, addr net.Addr, keys []string) error {
	return c.withAddrRwWithCtx(ctx, addr, func(_ net.Conn, rw *bufio.ReadWriter) error {
		for _, key := range keys {
			_, err := fmt.Fprintf(rw, "md %s q\r\n", key) // meta delete with "no reply"
			if err != nil {
				return err
			}
		}
		_, err := rw.Write(mn)
		if err != nil {
			return err
		}
		err = rw.Flush()
		if err != nil {
			return err
		}

		return parseMetaCommandResponse(rw)
	})
}

func (c *Client) SetMultiV2(ctx context.Context, items []*Item) error {
	itemsMap := make(map[net.Addr][]*Item)
	for _, item := range items {
		if !legalKey(item.Key) {
			return ErrMalformedKey
		}
		addr, err := c.selector.PickServer(item.Key)
		if err != nil {
			return err
		}
		itemsMap[addr] = append(itemsMap[addr], item)
	}

	ch := make(chan error, buffered)
	for addr, items := range itemsMap {
		go func(addr net.Addr, items []*Item) {
			ch <- c.setManyWithCtx(ctx, addr, items)
		}(addr, items)
	}

	var responseErr error
	for _ = range itemsMap {
		if ge := <-ch; ge != nil {
			responseErr = ge
		}
	}

	return responseErr
}

func (c *Client) setManyWithCtx(ctx context.Context, addr net.Addr, items []*Item) error {
	return c.withAddrRwWithCtx(ctx, addr, func(_ net.Conn, rw *bufio.ReadWriter) error {
		for _, item := range items {
			_, err := fmt.Fprintf(rw, "ms %s T%d S%d F%d q\r\n", item.Key, item.Expiration, len(item.Value), item.Flags)
			if err != nil {
				return err
			}
			_, err = rw.Write(item.Value)
			if err != nil {
				return err
			}
			_, err = rw.Write(crlf)
			if err != nil {
				return err
			}
		}
		_, err := rw.Write(mn)
		if err != nil {
			return err
		}
		err = rw.Flush()
		if err != nil {
			return err
		}

		return parseMetaCommandResponse(rw)
	})
}

func (c *Client) withAddrRwWithCtx(ctx context.Context, addr net.Addr, fn func(net.Conn, *bufio.ReadWriter) error) (err error) {
	cn, err := c.getConnWithContext(ctx, addr)
	if err != nil {
		return err
	}
	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)
		cn.SetTimeout(time.Millisecond * 500)
		err := fn(cn.NetConn(), cn.RW())
		errCh <- err
		if err == nil || resumableError(err) {
			cn.PutConn()
		} else {
			cn.Release(CloseReasonMemcachedError, err)
		}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		if err != nil {
			return errors.New("memcache: request error " + err.Error())
		}
		return nil
	}
}

func (c *Client) getFromAddrWithCtx(ctx context.Context, addr net.Addr, keys []string, cb func(*Item)) error {
	return c.withAddrRwWithCtx(ctx, addr, func(nc net.Conn, rw *bufio.ReadWriter) error {
		if _, err := fmt.Fprintf(rw, "gets %s\r\n", strings.Join(keys, " ")); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			rw.Writer.Reset(nc)
			return nil
		default:
		}

		if err := rw.Flush(); err != nil {
			return err
		}

		//TODO move this to func parseGetResponse
		select {
		case <-ctx.Done():
			return c.readUntilEnd(rw.Reader)
		default:
		}

		if err := parseGetResponse(rw.Reader, cb); err != nil {
			return err
		}
		return nil
	})
}

func (c *Client) readUntilEnd(r *bufio.Reader) error {
	for {
		line, err := r.ReadSlice('\n')
		if err != nil {
			return err
		}
		if bytes.Equal(line, resultEnd) {
			return nil
		}
	}
}
