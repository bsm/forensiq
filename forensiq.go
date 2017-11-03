// Package forensiq allows us to emit minutely/hourly stats to redis. Useful for forensic debugging.
package forensiq

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis"
	tomb "gopkg.in/tomb.v2"
)

const numShards = 64

type metricData map[string]map[string]float64

func (m metricData) Add(key, metric string, delta float64) {
	o, ok := m[key]
	if !ok {
		o = make(map[string]float64)
		m[key] = o
	}
	o[metric] += delta
}

// --------------------------------------------------------------------

// Client is responsible for accummulating and flushing data to redis
type Client struct {
	uc redis.UniversalClient
	sn [numShards]metricData
	mu [numShards]sync.Mutex
	si uint32

	ns string
	fi time.Duration
	tt time.Duration

	tm tomb.Tomb
}

// New inits a new Client
func New(namespace string, uc redis.UniversalClient, flushInterval, ttl time.Duration) *Client {
	c := &Client{
		uc: uc,
		ns: namespace,
		fi: flushInterval,
		tt: ttl,
	}
	for i := 0; i < numShards; i++ {
		c.sn[i] = make(metricData)
	}

	c.tm.Go(c.loop)
	return c
}

// Add increments a metric by delta
func (c *Client) Add(t time.Time, metric string, delta float64) {
	if c == nil {
		return
	}

	mkey := c.ns + t.UTC().Truncate(time.Minute).Format("2006-01-02|15:04")
	hkey := mkey[:len(mkey)-3]

	pos := int(atomic.AddUint32(&c.si, 1) % numShards)
	c.mu[pos].Lock()
	c.sn[pos].Add(mkey, metric, delta)
	c.sn[pos].Add(hkey, metric, delta)
	c.mu[pos].Unlock()
}

// Close closes the writer
func (c *Client) Close() error {
	if c == nil {
		return nil
	}

	c.tm.Kill(nil)
	return c.tm.Wait()
}

func (c *Client) flush() error {
	pipe := c.uc.Pipeline()
	defer pipe.Close()

	for i := range c.sn {
		vv := make(metricData)

		c.mu[i].Lock()
		vv, c.sn[i] = c.sn[i], vv
		c.mu[i].Unlock()

		for key, metrics := range vv {
			for metric, delta := range metrics {
				pipe.HIncrByFloat(key, metric, delta)
			}
			if c.tt > 0 {
				pipe.Expire(key, c.tt)
			}
		}
	}

	_, err := pipe.Exec()
	return err
}

func (c *Client) loop() error {
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.tm.Dying():
			return c.flush()
		case <-ticker.C:
			if err := c.flush(); err != nil {
				log.Printf("forensiq: unable to flush data: %s", err)
			}
		}
	}
}
