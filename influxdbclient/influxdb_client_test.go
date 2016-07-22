package influxdbclient_test

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"

	"github.com/andrew-edgar/influxdb-firehose-nozzle/influxdbclient"
	"github.com/cloudfoundry-incubator/datadog-firehose-nozzle/datadogclient"

	"github.com/cloudfoundry/gosteno"
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/gogo/protobuf/proto"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var (
	bodies       [][]byte
	responseCode int
)

var _ = Describe("DatadogClient", func() {
	var (
		ts  *httptest.Server
		log *gosteno.Logger
	)

	BeforeEach(func() {
		bodies = nil
		responseCode = http.StatusOK
		ts = httptest.NewServer(http.HandlerFunc(handlePost))
		log = gosteno.NewLogger("datadogclient test")
	})

	It("sends tags", func() {
		c := influxdbclient.New(ts.URL, "testdb", "user", "password", "influxdb.nozzle.", "test-deployment", "dummy-ip", log)

		c.AddMetric(&events.Envelope{
			Origin:    proto.String("test-origin"),
			Timestamp: proto.Int64(1000000000),
			EventType: events.Envelope_ValueMetric.Enum(),

			// fields that gets sent as tags
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("doppler"),
			Index:      proto.String("1"),
			Ip:         proto.String("10.0.1.2"),

			// additional tags
			Tags: map[string]string{
				"protocol":   "http",
				"request_id": "a1f5-deadbeef",
			},
		})

		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		Eventually(bodies).Should(HaveLen(1))
		var payload datadogclient.Payload
		err = json.Unmarshal(bodies[0], &payload)
		Expect(err).NotTo(HaveOccurred())
		Expect(payload.Series).To(HaveLen(4))

		var metric datadogclient.Metric
		Expect(metric.Tags).To(ConsistOf(
			"deployment:deployment-name",
			"job:doppler",
			"index:1",
			"ip:10.0.1.2",
			"protocol:http",
			"request_id:a1f5-deadbeef",
		))
	})

	It("uses tags as an identifier for batching purposes", func() {
		c := influxdbclient.New(ts.URL, "testdb", "user", "password", "influxdb.nozzle.", "test-deployment", "dummy-ip", log)

		c.AddMetric(&events.Envelope{
			Origin:    proto.String("test-origin"),
			Timestamp: proto.Int64(1000000000),
			EventType: events.Envelope_ValueMetric.Enum(),

			// fields that gets sent as tags
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("doppler"),
			Index:      proto.String("1"),
			Ip:         proto.String("10.0.1.2"),

			// additional tags
			Tags: map[string]string{
				"protocol":   "http",
				"request_id": "a1f5-deadbeef",
			},
		})

		c.AddMetric(&events.Envelope{
			Origin:    proto.String("test-origin"),
			Timestamp: proto.Int64(1000000000),
			EventType: events.Envelope_ValueMetric.Enum(),

			// fields that gets sent as tags
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("doppler"),
			Index:      proto.String("1"),
			Ip:         proto.String("10.0.1.2"),

			// additional tags
			Tags: map[string]string{
				"protocol":   "https",
				"request_id": "d3ac-livefood",
			},
		})

		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		Eventually(bodies).Should(HaveLen(1))
		var payload datadogclient.Payload
		err = json.Unmarshal(bodies[0], &payload)
		Expect(err).NotTo(HaveOccurred())
		Expect(payload.Series).To(HaveLen(5))
	})

	It("ignores messages that aren't value metrics or counter events", func() {
		c := influxdbclient.New(ts.URL, "testdb", "user", "password", "influxdb.nozzle.", "test-deployment", "dummy-ip", log)

		c.AddMetric(&events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(1000000000),
			EventType: events.Envelope_LogMessage.Enum(),
			LogMessage: &events.LogMessage{
				Message:     []byte("log message"),
				MessageType: events.LogMessage_OUT.Enum(),
				Timestamp:   proto.Int64(1000000000),
			},
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("doppler"),
		})

		c.AddMetric(&events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(1000000000),
			EventType: events.Envelope_ContainerMetric.Enum(),
			ContainerMetric: &events.ContainerMetric{
				ApplicationId: proto.String("app-id"),
				InstanceIndex: proto.Int32(4),
				CpuPercentage: proto.Float64(20.0),
				MemoryBytes:   proto.Uint64(19939949),
				DiskBytes:     proto.Uint64(29488929),
			},
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("doppler"),
		})

		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		Eventually(bodies).Should(HaveLen(1))
	})

	It("generates aggregate messages even when idle", func() {
		c := influxdbclient.New(ts.URL, "testdb", "user", "password", "influxdb.nozzle.", "test-deployment", "dummy-ip", log)

		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		Eventually(bodies).Should(HaveLen(1))

		err = c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		Eventually(bodies).Should(HaveLen(2))
	})

	It("posts ValueMetrics in JSON format", func() {
		c := influxdbclient.New(ts.URL, "testdb", "user", "password", "influxdb.nozzle.", "test-deployment", "dummy-ip", log)

		c.AddMetric(&events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(1000000000),
			EventType: events.Envelope_ValueMetric.Enum(),
			ValueMetric: &events.ValueMetric{
				Name:  proto.String("metricName"),
				Value: proto.Float64(5),
			},
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("doppler"),
		})

		c.AddMetric(&events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(2000000000),
			EventType: events.Envelope_ValueMetric.Enum(),
			ValueMetric: &events.ValueMetric{
				Name:  proto.String("metricName"),
				Value: proto.Float64(76),
			},
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("doppler"),
		})

		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		Eventually(bodies).Should(HaveLen(1))
	})

	It("registers metrics with the same name but different tags as different", func() {
		c := influxdbclient.New(ts.URL, "testdb", "user", "password", "influxdb.nozzle.", "test-deployment", "dummy-ip", log)

		c.AddMetric(&events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(1000000000),
			EventType: events.Envelope_ValueMetric.Enum(),
			ValueMetric: &events.ValueMetric{
				Name:  proto.String("metricName"),
				Value: proto.Float64(5),
			},
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("doppler"),
		})

		c.AddMetric(&events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(2000000000),
			EventType: events.Envelope_ValueMetric.Enum(),
			ValueMetric: &events.ValueMetric{
				Name:  proto.String("metricName"),
				Value: proto.Float64(76),
			},
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("gorouter"),
		})

		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		Eventually(bodies).Should(HaveLen(1))
	})

	It("posts CounterEvents in JSON format and empties map after post", func() {
		c := influxdbclient.New(ts.URL, "testdb", "user", "password", "influxdb.nozzle.", "test-deployment", "dummy-ip", log)

		c.AddMetric(&events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(1000000000),
			EventType: events.Envelope_CounterEvent.Enum(),
			CounterEvent: &events.CounterEvent{
				Name:  proto.String("counterName"),
				Delta: proto.Uint64(1),
				Total: proto.Uint64(5),
			},
		})

		c.AddMetric(&events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(2000000000),
			EventType: events.Envelope_CounterEvent.Enum(),
			CounterEvent: &events.CounterEvent{
				Name:  proto.String("counterName"),
				Delta: proto.Uint64(6),
				Total: proto.Uint64(11),
			},
		})

		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		Eventually(bodies).Should(HaveLen(1))
	})

	It("sends a value 1 for the slowConsumerAlert metric when consumer error is set", func() {
		c := influxdbclient.New(ts.URL, "testdb", "user", "password", "influxdb.nozzle.", "test-deployment", "dummy-ip", log)

		c.AlertSlowConsumerError()

		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		Eventually(bodies).Should(HaveLen(1))
	})

	It("sends a value 0 for the slowConsumerAlert metric when consumer error is not set", func() {
		c := influxdbclient.New(ts.URL, "testdb", "user", "password", "influxdb.nozzle.", "test-deployment", "dummy-ip", log)

		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		Eventually(bodies).Should(HaveLen(1))
	})

	It("unsets the slow consumer error once it publishes the alert to datadog", func() {
		c := influxdbclient.New(ts.URL, "testdb", "user", "password", "influxdb.nozzle.", "test-deployment", "dummy-ip", log)

		c.AlertSlowConsumerError()

		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		Eventually(bodies).Should(HaveLen(1))
	})

	It("returns an error when datadog responds with a non 200 response code", func() {
		c := influxdbclient.New(ts.URL, "testdb", "user", "password", "influxdb.nozzle.", "test-deployment", "dummy-ip", log)

		responseCode = http.StatusBadRequest // 400
		err := c.PostMetrics()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("datadog request returned HTTP response: 400 Bad Request"))

		responseCode = http.StatusSwitchingProtocols // 101
		err = c.PostMetrics()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("datadog request returned HTTP response: 101"))

		responseCode = http.StatusAccepted // 201
		err = c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())
	})
})

func handlePost(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic("No body!")
	}

	bodies = append(bodies, body)
	w.WriteHeader(responseCode)
}
