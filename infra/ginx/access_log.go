package ginx

import (
	"bytes"
	"encoding/base64"
	"io"
	"net/http"
	"unicode/utf8"
)

type requestInfo struct {
	Method       string      `json:"method"`
	Headers      http.Header `json:"headers"`
	Body         string      `json:"body"`
	BodyEncoding string      `json:"body_encoding"`
	BodyError    string      `json:"body_error,omitempty"`
}

type responseInfo struct {
	Headers      http.Header `json:"headers"`
	Body         string      `json:"body"`
	BodyEncoding string      `json:"body_encoding"`
}

func captureRequestInfo(req *http.Request) *requestInfo {
	headers := req.Header.Clone()
	if headers == nil {
		headers = make(http.Header)
	}
	info := &requestInfo{
		Method:       req.Method,
		Headers:      headers,
		BodyEncoding: "utf-8",
	}
	if req.Body == nil || req.Body == http.NoBody {
		return info
	}
	body, err := io.ReadAll(req.Body)
	info.Body, info.BodyEncoding = bodyForLog(body)
	if err != nil {
		info.BodyError = err.Error()
	}
	// 完整详情需要预读;还原已读字节和失败结果,避免业务误把读取失败当成 EOF。
	req.Body = &replayBody{
		ReadCloser: req.Body,
		buffer:     *bytes.NewReader(body),
		err:        err,
	}
	return info
}

type replayBody struct {
	io.ReadCloser
	buffer bytes.Reader
	err    error
}

func (b *replayBody) Read(p []byte) (int, error) {
	if b.buffer.Len() > 0 {
		return b.buffer.Read(p)
	}
	if b.err != nil {
		err := b.err
		b.err = nil
		return 0, err
	}
	return b.ReadCloser.Read(p)
}

func bodyForLog(body []byte) (string, string) {
	if utf8.Valid(body) {
		return string(body), "utf-8"
	}
	return base64.StdEncoding.EncodeToString(body), "base64"
}

func (w *responseWriter) captureHeaders() {
	if w.body != nil && w.headers == nil {
		// 响应提交后的 Header 修改不会发给客户端,因此只保留首次提交时的快照。
		w.headers = w.Header().Clone()
	}
}

func (w *responseWriter) Write(p []byte) (int, error) {
	w.captureHeaders()
	n, err := w.ResponseWriter.Write(p)
	if w.body != nil {
		_, _ = w.body.Write(p[:n])
	}
	return n, err
}

func (w *responseWriter) WriteString(s string) (int, error) {
	w.captureHeaders()
	n, err := w.ResponseWriter.WriteString(s)
	if w.body != nil {
		_, _ = w.body.WriteString(s[:n])
	}
	return n, err
}

func (w *responseWriter) WriteHeaderNow() {
	w.captureHeaders()
	w.ResponseWriter.WriteHeaderNow()
}

func (w *responseWriter) Flush() {
	w.captureHeaders()
	w.ResponseWriter.Flush()
}
