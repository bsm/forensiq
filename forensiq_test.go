package forensiq_test

import (
	"os"
	"testing"
	"time"

	"github.com/bsm/forensiq"
	"github.com/go-redis/redis"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Client", func() {
	var subject *forensiq.Client
	var client redis.UniversalClient
	const ttl = 2 * time.Hour

	BeforeEach(func() {
		addr := os.Getenv("REDIS_ADDR")
		if addr == "" {
			addr = "localhost:6379"
		}
		client = redis.NewUniversalClient(&redis.UniversalOptions{
			Addrs: []string{addr},
			DB:    9,
		})

		subject = forensiq.New("forensiq:", client, time.Minute, ttl)
		subject.Add(time.Unix(1515151515, 0), "a", 1.2)
		subject.Add(time.Unix(1515151516, 0), "a", 1.3)
		subject.Add(time.Unix(1515151600, 0), "a", 1.4)
		subject.Add(time.Unix(1515155500, 0), "a", 1.5)
		subject.Add(time.Unix(1515151515, 0), "b", 2.1)
		subject.Add(time.Unix(1515151516, 0), "b", 2.2)
	})

	AfterEach(func() {
		Expect(subject.Close()).To(Succeed())
		Expect(client.FlushDB().Err()).To(Succeed())
		Expect(client.Close()).To(Succeed())
	})

	It("should flush data", func() {
		Expect(subject.Flush()).To(Succeed())

		Expect(client.Keys("*").Result()).To(ConsistOf(
			"forensiq:2018-01-05|11",
			"forensiq:2018-01-05|11:25",
			"forensiq:2018-01-05|11:26",
			"forensiq:2018-01-05|12",
			"forensiq:2018-01-05|12:31",
		))
		Expect(client.HGetAll("forensiq:2018-01-05|11").Result()).To(Equal(map[string]string{"a": "3.9", "b": "4.3"}))
		Expect(client.HGetAll("forensiq:2018-01-05|11:25").Result()).To(Equal(map[string]string{"a": "2.5", "b": "4.3"}))
		Expect(client.HGetAll("forensiq:2018-01-05|11:26").Result()).To(Equal(map[string]string{"a": "1.4"}))
		Expect(client.HGetAll("forensiq:2018-01-05|12").Result()).To(Equal(map[string]string{"a": "1.5"}))
		Expect(client.HGetAll("forensiq:2018-01-05|12:31").Result()).To(Equal(map[string]string{"a": "1.5"}))

		Expect(client.TTL("forensiq:2018-01-05|11").Result()).To(BeNumerically("~", ttl, 5*time.Second))
		Expect(client.TTL("forensiq:2018-01-05|11:25").Result()).To(BeNumerically("~", ttl, 5*time.Second))
		Expect(client.TTL("forensiq:2018-01-05|12:31").Result()).To(BeNumerically("~", ttl, 5*time.Second))

		subject.Add(time.Unix(1515155510, 0), "a", 1.6)
		Expect(subject.Flush()).To(Succeed())
		Expect(client.HGetAll("forensiq:2018-01-05|12:31").Result()).To(Equal(map[string]string{"a": "3.1"}))
	})

})

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "forensiq")
}
