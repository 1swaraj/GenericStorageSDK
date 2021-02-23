package requestlog

import (
	"io"
	"strconv"
	"sync"
)

type NCSALogger struct {
	onErr func(error)

	mu  sync.Mutex
	w   io.Writer
	buf []byte
}

func NewNCSALogger(w io.Writer, onErr func(error)) *NCSALogger {
	return &NCSALogger{
		w:     w,
		onErr: onErr,
	}
}

func (l *NCSALogger) Log(ent *Entry) {
	if err := l.log(ent); err != nil && l.onErr != nil {
		l.onErr(err)
	}
}

func (l *NCSALogger) log(ent *Entry) error {
	defer l.mu.Unlock()
	l.mu.Lock()
	l.buf = formatEntry(l.buf[:0], ent)
	_, err := l.w.Write(l.buf)
	return err
}

func formatEntry(b []byte, ent *Entry) []byte {
	const ncsaTime = "02/Jan/2006:15:04:05 -0700"
	if ent.RemoteIP == "" {
		b = append(b, '-')
	} else {
		b = append(b, ent.RemoteIP...)
	}
	b = append(b, " - - ["...)
	b = ent.ReceivedTime.AppendFormat(b, ncsaTime)
	b = append(b, "] \""...)
	b = append(b, ent.RequestMethod...)
	b = append(b, ' ')
	b = append(b, ent.RequestURL...)
	b = append(b, ' ')
	b = append(b, ent.Proto...)
	b = append(b, "\" "...)
	b = strconv.AppendInt(b, int64(ent.Status), 10)
	b = append(b, ' ')
	b = strconv.AppendInt(b, int64(ent.ResponseBodySize), 10)
	b = append(b, ' ')
	b = strconv.AppendQuote(b, ent.Referer)
	b = append(b, ' ')
	b = strconv.AppendQuote(b, ent.UserAgent)
	b = append(b, '\n')
	return b
}
