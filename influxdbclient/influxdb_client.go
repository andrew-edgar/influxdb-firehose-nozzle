package influxdbclient

import (
	"bytes"
	"crypto/sha1"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/cloudfoundry/gosteno"
	"github.com/cloudfoundry/sonde-go/events"
)

type Client struct {
	url                   string
	database              string
	user                  string
	password              string
	allowSelfSigned       bool
	metricPoints          map[metricKey]metricValue
	prefix                string
	deployment            string
	ip                    string
	tagsHash              string
	totalMessagesReceived uint64
	totalMetricsSent      uint64
	log                   *gosteno.Logger
}

type metricKey struct {
	eventType events.Envelope_EventType
	name      string
	tagsHash  string
}

type metricValue struct {
	tags   []string
	points []Point
}

type Metric struct {
	Metric string   `json:"metric"`
	Points []Point  `json:"points"`
	Type   string   `json:"type"`
	Host   string   `json:"host,omitempty"`
	Tags   []string `json:"tags,omitempty"`
}

type Point struct {
	Timestamp int64
	Value     float64
}

func New(url string, database string, user string, password string, allowSelfSigned bool, prefix string, deployment string, ip string, log *gosteno.Logger) *Client {
	return &Client{
		url:             url,
		database:        database,
		user:            user,
		password:        password,
		allowSelfSigned: allowSelfSigned,
		metricPoints:    make(map[metricKey]metricValue),
		prefix:          prefix,
		deployment:      deployment,
		ip:              ip,
		log:             log,
	}
}

func (c *Client) AlertSlowConsumerError() {
	c.addInternalMetric("slowConsumerAlert", uint64(1))
}

func (c *Client) AddMetric(envelope *events.Envelope) {
	c.totalMessagesReceived++
	if envelope.GetEventType() != events.Envelope_ValueMetric && envelope.GetEventType() != events.Envelope_CounterEvent {
		return
	}

	tags := parseTags(envelope)
	key := metricKey{
		eventType: envelope.GetEventType(),
		name:      getName(envelope),
		tagsHash:  hashTags(tags),
	}

	mVal := c.metricPoints[key]
	value := getValue(envelope)

	mVal.tags = tags
	mVal.points = append(mVal.points, Point{
		Timestamp: envelope.GetTimestamp() / int64(time.Second),
		Value:     value,
	})

	// c.log.Infof("got-metric(%s): %v", key, mVal)

	c.metricPoints[key] = mVal
}

func (c *Client) PostMetrics() error {
	url := c.seriesURL()

	c.populateInternalMetrics()
	numMetrics := len(c.metricPoints)
	c.log.Infof("Posting %d metrics", numMetrics)

	seriesBytes, metricsCount := c.formatMetrics()

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	httpClient := &http.Client{Transport: tr}

	resp, err := httpClient.Post(url, "application/binary", bytes.NewBuffer(seriesBytes))
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode >= 300 || resp.StatusCode < 200 {
		errBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("Can't read response body: %s", resp.Status)
		}
		return fmt.Errorf("InfluxDB request returned HTTP response: %s;\n%s", resp.Status, string(errBody))
	}

	c.totalMetricsSent += metricsCount
	c.metricPoints = make(map[metricKey]metricValue)

	return nil
}

func (c *Client) seriesURL() string {
	url := fmt.Sprintf("%s/write?db=%s", c.url, c.database)
	c.log.Info("Using the following influx URL " + url)
	return url
}

func (c *Client) populateInternalMetrics() {
	c.addInternalMetric("totalMessagesReceived", c.totalMessagesReceived)
	c.addInternalMetric("totalMetricsSent", c.totalMetricsSent)

	if !c.containsSlowConsumerAlert() {
		c.addInternalMetric("slowConsumerAlert", uint64(0))
	}
}

func (c *Client) containsSlowConsumerAlert() bool {
	key := metricKey{
		name:     "slowConsumerAlert",
		tagsHash: c.tagsHash,
	}
	_, ok := c.metricPoints[key]
	return ok
}

func (c *Client) formatMetrics() ([]byte, uint64) {
	var buffer bytes.Buffer

	for key, mVal := range c.metricPoints {
		mVal.tags = append(mVal.tags, "potato=face")
		buffer.WriteString(c.prefix + key.name)
		if len(mVal.tags) > 0 {
			buffer.WriteString(",")
			buffer.WriteString(formatTags(mVal.tags))
		}
		buffer.WriteString(" ")
		buffer.WriteString(formatValues(mVal.points))
		buffer.WriteString(" ")
		buffer.WriteString(formatTimestamp(mVal.points))
		buffer.WriteString("\n")
	}

	return buffer.Bytes(), uint64(len(c.metricPoints))
}

func formatTags(tags []string) string {
	var newTags string
	for index, tag := range tags {
		if index > 0 {
			newTags += ","
		}

		newTags += tag
	}
	return newTags
}

func formatValues(points []Point) string {
	var newPoints string
	for index, point := range points {
		if index > 0 {
			newPoints += ","
		}

		newPoints += "value=" + strconv.FormatFloat(point.Value, 'f', -1, 64)
	}
	return newPoints
}

func formatTimestamp(points []Point) string {
	if len(points) > 0 {
		return strconv.FormatInt(points[0].Timestamp*1000*1000*1000, 10)
	} else {
		return strconv.FormatInt(time.Now().Unix()*1000*1000*1000, 10)
	}
}

func (c *Client) addInternalMetric(name string, value uint64) {
	key := metricKey{
		name:     name,
		tagsHash: c.tagsHash,
	}

	point := Point{
		Timestamp: time.Now().Unix(),
		Value:     float64(value),
	}

	mValue := metricValue{
		tags: []string{
			fmt.Sprintf("ip=%s", c.ip),
			fmt.Sprintf("deployment=%s", c.deployment),
		},
		points: []Point{point},
	}

	c.metricPoints[key] = mValue
}

func getName(envelope *events.Envelope) string {
	switch envelope.GetEventType() {
	case events.Envelope_ValueMetric:
		return envelope.GetOrigin() + "." + envelope.GetValueMetric().GetName()
	case events.Envelope_CounterEvent:
		return envelope.GetOrigin() + "." + envelope.GetCounterEvent().GetName()
	default:
		panic("Unknown event type")
	}
}

func getValue(envelope *events.Envelope) float64 {
	switch envelope.GetEventType() {
	case events.Envelope_ValueMetric:
		return envelope.GetValueMetric().GetValue()
	case events.Envelope_CounterEvent:
		return float64(envelope.GetCounterEvent().GetTotal())
	default:
		panic("Unknown event type")
	}
}

func parseTags(envelope *events.Envelope) []string {
	tags := appendTagIfNotEmpty(nil, "deployment", envelope.GetDeployment())
	tags = appendTagIfNotEmpty(tags, "job", envelope.GetJob())
	tags = appendTagIfNotEmpty(tags, "index", envelope.GetIndex())
	tags = appendTagIfNotEmpty(tags, "ip", envelope.GetIp())
	for tname, tvalue := range envelope.GetTags() {
		tags = appendTagIfNotEmpty(tags, tname, tvalue)
	}
	return tags
}

func appendTagIfNotEmpty(tags []string, key, value string) []string {
	if value != "" {
		tags = append(tags, fmt.Sprintf("%s=%s", key, value))
	}
	return tags
}

func hashTags(tags []string) string {
	sort.Strings(tags)
	hash := ""
	for _, tag := range tags {
		tagHash := sha1.Sum([]byte(tag))
		hash += string(tagHash[:])
	}
	return hash
}
