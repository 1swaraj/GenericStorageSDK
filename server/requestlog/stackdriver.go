package requestlog

import (
	"bytes"
	"encoding/json"
	"io"
	"strconv"
	"sync"
	"time"
)

type StackdriverLogger struct {
	onErr func(error)

	mu  sync.Mutex
	w   io.Writer
	buf bytes.Buffer
	enc *json.Encoder
}

func NewStackdriverLogger(w io.Writer, onErr func(error)) *StackdriverLogger {
	l := &StackdriverLogger{
		w:     w,
		onErr: onErr,
	}
	l.enc = json.NewEncoder(&l.buf)
	return l
}

func (l *StackdriverLogger) Log(ent *Entry) {
	if err := l.log(ent); err != nil && l.onErr != nil {
		l.onErr(err)
	}
}

func (l *StackdriverLogger) log(ent *Entry) error {
	defer l.mu.Unlock()
	l.mu.Lock()

	l.buf.Reset()

	var r struct {
		HTTPRequest struct {
			RequestMethod string `json:"requestMethod"`
			RequestURL    string `json:"requestUrl"`
			RequestSize   int64  `json:"requestSize,string"`
			Status        int    `json:"status"`
			ResponseSize  int64  `json:"responseSize,string"`
			UserAgent     string `json:"userAgent"`
			RemoteIP      string `json:"remoteIp"`
			Referer       string `json:"referer"`
			Latency       string `json:"latency"`
		} `json:"httpRequest"`
		Timestamp struct {
			Seconds int64 `json:"seconds"`
			Nanos   int   `json:"nanos"`
		} `json:"timestamp"`
		TraceID string `json:"logging.googleapis.com/trace"`
		SpanID  string `json:"logging.googleapis.com/spanId"`
	}
	r.HTTPRequest.RequestMethod = ent.RequestMethod
	r.HTTPRequest.RequestURL = ent.RequestURL

	r.HTTPRequest.RequestSize = ent.RequestHeaderSize + ent.RequestBodySize
	r.HTTPRequest.Status = ent.Status

	r.HTTPRequest.ResponseSize = ent.ResponseHeaderSize + ent.ResponseBodySize
	r.HTTPRequest.UserAgent = ent.UserAgent
	r.HTTPRequest.RemoteIP = ent.RemoteIP
	r.HTTPRequest.Referer = ent.Referer
	r.HTTPRequest.Latency = string(appendLatency(nil, ent.Latency))

	t := ent.ReceivedTime.Add(ent.Latency)
	r.Timestamp.Seconds = t.Unix()
	r.Timestamp.Nanos = t.Nanosecond()
	r.TraceID = ent.TraceID.String()
	r.SpanID = ent.SpanID.String()
	if err := l.enc.Encode(r); err != nil {
		return err
	}
	_, err := l.w.Write(l.buf.Bytes())

	return err
}

func appendLatency(b []byte, d time.Duration) []byte {

	b = strconv.AppendFloat(b, d.Seconds(), 'f', 9, 64)
	b = append(b, 's')
	return b
}
