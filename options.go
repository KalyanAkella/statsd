package statsd

import (
	"bytes"
	"strings"
	"time"
)

type config struct {
	Conn   connConfig
	Client clientConfig
}

type clientConfig struct {
	Muted  bool
	Rate   float32
	Prefix string
	Tags   *Tags
}

type connConfig struct {
	Addr          string
	ErrorHandler  func(error)
	FlushPeriod   time.Duration
	MaxPacketSize int
	Network       string
	TagFormat     TagFormat
}

// An Option represents an option for a Client. It must be used as an
// argument to New() or Client.Clone().
type Option func(*config)

// Address sets the address of the StatsD daemon.
//
// By default, ":8125" is used. This option is ignored in Client.Clone().
func Address(addr string) Option {
	return Option(func(c *config) {
		c.Conn.Addr = addr
	})
}

// ErrorHandler sets the function called when an error happens when sending
// metrics (e.g. the StatsD daemon is not listening anymore).
//
// By default, these errors are ignored.  This option is ignored in
// Client.Clone().
func ErrorHandler(h func(error)) Option {
	return Option(func(c *config) {
		c.Conn.ErrorHandler = h
	})
}

// FlushPeriod sets how often the Client's buffer is flushed. If p is 0, the
// goroutine that periodically flush the buffer is not lauched and the buffer
// is only flushed when it is full.
//
// By default, the flush period is 100 ms.  This option is ignored in
// Client.Clone().
func FlushPeriod(p time.Duration) Option {
	return Option(func(c *config) {
		c.Conn.FlushPeriod = p
	})
}

// MaxPacketSize sets the maximum packet size in bytes sent by the Client.
//
// By default, it is 1440 to avoid IP fragmentation. This option is ignored in
// Client.Clone().
func MaxPacketSize(n int) Option {
	return Option(func(c *config) {
		c.Conn.MaxPacketSize = n
	})
}

// Network sets the network (udp, tcp, etc) used by the client. See the
// net.Dial documentation (https://golang.org/pkg/net/#Dial) for the available
// network options.
//
// By default, network is udp. This option is ignored in Client.Clone().
func Network(network string) Option {
	return Option(func(c *config) {
		c.Conn.Network = network
	})
}

// Mute sets whether the Client is muted. All methods of a muted Client do
// nothing and return immedialtly.
//
// This option can be used in Client.Clone() only if the parent Client is not
// muted. The clones of a muted Client are always muted.
func Mute(b bool) Option {
	return Option(func(c *config) {
		c.Client.Muted = b
	})
}

// SampleRate sets the sample rate of the Client. It allows sending the metrics
// less often which can be useful for performance intensive code paths.
func SampleRate(rate float32) Option {
	return Option(func(c *config) {
		c.Client.Rate = rate
	})
}

// Prefix appends the prefix that will be used in every bucket name.
//
// Note that when used in cloned, the prefix of the parent Client is not
// replaced but is prepended to the given prefix.
func Prefix(p string) Option {
	return Option(func(c *config) {
		c.Client.Prefix += strings.TrimSuffix(p, ".") + "."
	})
}

// TagFormat represents the format of tags sent by a Client.
type TagFormat uint8

// TagsFormat sets the format of tags.
func TagsFormat(tf TagFormat) Option {
	return Option(func(c *config) {
		c.Conn.TagFormat = tf
	})
}

// CommonTags appends the given tags to the tags sent with every metrics. If a tag
// already exists, it is replaced.
//
// The tags must be set as key-value pairs. If the number of tags is not even,
// CommonTags panics.
//
// If the format of tags have not been set using the TagsFormat option, the tags
// will be ignored.
func CommonTags(tags ...string) Option {
	if len(tags)%2 != 0 {
		panic("statsd: Tags only accepts an even number of arguments")
	}

	return Option(func(c *config) {
		if len(tags) == 0 {
			return
		}

		newCommonTags := newTags(tags...)
		c.Client.Tags.append(newCommonTags)
	})
}

type Tags struct {
	kvs  map[string]string
	keys []string
}

func emptyTags() *Tags {
	kvs := make(map[string]string)
	keys := make([]string, 0)
	return &Tags{kvs, keys}
}

func newTags(tags ...string) *Tags {
	if len(tags)&1 == 1 {
		panic("statsd: newTags only accepts an even number of arguments")
	}

	if len(tags) == 0 {
		return emptyTags()
	}

	numTags := len(tags) >> 1
	tagKeys := make([]string, numTags)
	newTags := make(map[string]string, numTags)
	for i := 0; i < numTags; i++ {
		k, v := tags[i<<1], tags[1+i<<1]
		newTags[k] = v
		tagKeys[i] = k
	}

	return &Tags{newTags, tagKeys}
}

func (this *Tags) append(that *Tags) {
	for _, k := range that.keys {
		if _, present := this.kvs[k]; !present {
			this.kvs[k] = that.kvs[k]
			this.keys = append(this.keys, k)
		}
	}
}

func (this *Tags) numTags() int {
	return len(this.keys)
}

func (this *Tags) clone() *Tags {
	numTags := this.numTags()
	kvs := make(map[string]string, numTags)
	keys := make([]string, numTags)
	for i, k := range this.keys {
		kvs[k] = this.kvs[k]
		keys[i] = k
	}
	return &Tags{kvs, keys}
}

func (this *Tags) format(tf TagFormat) string {
	if this.numTags() == 0 {
		return ""
	}
	switch tf {
	case InfluxDB:
		var buf bytes.Buffer
		for _, k := range this.keys {
			_ = buf.WriteByte(',')
			_, _ = buf.WriteString(k)
			_ = buf.WriteByte('=')
			_, _ = buf.WriteString(this.kvs[k])
		}
		return buf.String()
	case Datadog:
		buf := bytes.NewBufferString("|#")
		for _, k := range this.keys {
			_, _ = buf.WriteString(k)
			_ = buf.WriteByte(':')
			_, _ = buf.WriteString(this.kvs[k])
			_ = buf.WriteByte(',')
		}
		result := buf.String()
		return strings.TrimSuffix(result, ",")
	default:
		return ""
	}
}

const (
	// InfluxDB tag format.
	// See https://influxdb.com/blog/2015/11/03/getting_started_with_influx_statsd.html
	InfluxDB TagFormat = iota + 1
	// Datadog tag format.
	// See http://docs.datadoghq.com/guides/metrics/#tags
	Datadog
)

func (tf TagFormat) split(s string) *Tags {
	if len(s) == 0 {
		return emptyTags()
	}
	switch tf {
	case InfluxDB:
		s = s[1:]
		pairs := strings.Split(s, ",")
		keys := make([]string, len(pairs))
		tags := make(map[string]string, len(pairs))
		for i, pair := range pairs {
			kv := strings.Split(pair, "=")
			tags[kv[0]] = kv[1]
			keys[i] = kv[0]
		}
		return &Tags{tags, keys}
	case Datadog:
		s = s[2:]
		pairs := strings.Split(s, ",")
		keys := make([]string, len(pairs))
		tags := make(map[string]string, len(pairs))
		for i, pair := range pairs {
			kv := strings.Split(pair, ":")
			tags[kv[0]] = kv[1]
			keys[i] = kv[0]
		}
		return &Tags{tags, keys}
	default:
		return emptyTags()
	}
}
