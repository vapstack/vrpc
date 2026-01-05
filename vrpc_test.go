package vrpc

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/rpc"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type Request struct{ A, B int }
type Response struct{ Sum int }

type MathService struct {
	lastSum atomic.Int64 // beacon/notify side effect
	wg      sync.WaitGroup
}

func (s *MathService) Add(ctx context.Context, req *Request) (*Response, error) {
	if req.A == -1 {
		return nil, errors.New("simulated error")
	}
	if req.A == -666 {
		panic("simulated panic")
	}

	sum := req.A + req.B
	s.lastSum.Store(int64(sum))
	return &Response{Sum: sum}, nil
}

func setupServer(t *testing.T) (*MathService, *httptest.Server) {
	t.Helper()

	svc := new(MathService)
	handler, err := NewHandlerFor[MathService](svc)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	mux, err := NewMux(handler)
	if err != nil {
		t.Fatalf("failed to create mux: %v", err)
	}
	server := httptest.NewServer(mux)
	return svc, server
}

func setupClientToServer(t *testing.T, serverURL string, opts ...ClientOption) *Client {
	t.Helper()

	base := []ClientOption{WithEndpoint(serverURL)}
	base = append(base, opts...)

	c, err := NewClient(base...)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	return c
}

func TestCall_Success(t *testing.T) {
	_, server := setupServer(t)
	defer server.Close()

	client := setupClientToServer(t, server.URL)

	res := new(Response)
	err := client.Call(context.Background(), "MathService", "Add", &Request{A: 10, B: 20}, res)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Sum != 30 {
		t.Errorf("expected 30, got %d", res.Sum)
	}
}

func TestCall_ServiceError(t *testing.T) {
	_, server := setupServer(t)
	defer server.Close()

	client := setupClientToServer(t, server.URL)

	res := new(Response)
	err := client.Call(context.Background(), "MathService", "Add", &Request{A: -1, B: 20}, res)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "simulated error" {
		t.Errorf("expected 'simulated error', got '%v'", err)
	}
}

func TestCall_ServicePanic(t *testing.T) {
	_, server := setupServer(t)
	defer server.Close()

	client := setupClientToServer(t, server.URL)

	res := new(Response)
	err := client.Call(context.Background(), "MathService", "Add", &Request{A: -666, B: 20}, res)
	if err == nil {
		t.Fatal("expected error from panic, got nil")
	}
}

func TestNotify(t *testing.T) {
	svc, server := setupServer(t)
	defer server.Close()

	client := setupClientToServer(t, server.URL)

	err := client.Notify(context.Background(), "MathService", "Add", &Request{A: 5, B: 5})
	if err != nil {
		t.Fatalf("notify failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	if val := svc.lastSum.Load(); val != 10 {
		t.Errorf("expected side effect 10, got %d", val)
	}
}

func TestBeacon(t *testing.T) {
	svc, server := setupServer(t)
	defer server.Close()

	client := setupClientToServer(t, server.URL)

	err := client.Beacon(context.Background(), "MathService", "Add", &Request{A: 50, B: 50})
	if err != nil {
		t.Fatalf("beacon failed: %v", err)
	}

	start := time.Now()
	for {
		if svc.lastSum.Load() == 100 {
			break
		}
		if time.Since(start) > 500*time.Millisecond {
			t.Fatal("timeout waiting for beacon execution")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestRace(t *testing.T) {
	_, server := setupServer(t)
	defer server.Close()

	client := setupClientToServer(t, server.URL)

	var wg sync.WaitGroup
	workers := 20
	iterations := 1000

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				req := &Request{A: id, B: j}
				if j%2 == 0 {
					_ = client.Beacon(context.Background(), "MathService", "Add", req)
				} else {
					res := new(Response)
					_ = client.Call(context.Background(), "MathService", "Add", req, res)
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestServiceToHeader(t *testing.T) {
	svc := new(MathService)
	h, err := NewHandlerFor[MathService](svc)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	mux, err := NewMux(h)
	if err != nil {
		t.Fatalf("failed to create mux: %v", err)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "MathService" {
			t.Fatalf("expected Host header %q, got %q", "MathService", r.Host)
		}
		mux.ServeHTTP(w, r)
	}))
	defer ts.Close()

	clientHTTP := &http.Client{
		Transport: &http.Transport{
			DisableCompression: true,
			DialContext: func(_ context.Context, network, _ string) (net.Conn, error) {
				return net.Dial(network, ts.Listener.Addr().String())
			},
		},
	}

	client, err := NewClient(
		WithEndpoint("http://ignored-base-url"),
		WithMode(ServiceToHeader),
		WithClient(clientHTTP),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	res := new(Response)
	err = client.Call(context.Background(), "MathService", "Add", &Request{A: 10, B: 20}, res)
	if err != nil {
		t.Fatalf("ServiceToHeader call failed: %v", err)
	}
	if res.Sum != 30 {
		t.Fatalf("unexpected result: %d", res.Sum)
	}
}

func TestServiceToURL(t *testing.T) {
	svc := new(MathService)
	h, err := NewHandlerOf(svc)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	baseMux, err := NewMux(h)
	if err != nil {
		t.Fatalf("failed to create mux: %v", err)
	}

	const prefix = "/prefix"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "MathService" {
			t.Fatalf("expected Host header %q, got %q", "MathService", r.Host)
		}
		if r.URL.Path != prefix+"/MathService/Add" {
			t.Fatalf("expected path %q, got %q", prefix+"/MathService/Add", r.URL.Path)
		}

		r.URL.Path = strings.TrimPrefix(r.URL.Path, prefix)
		if r.URL.Path == "" {
			r.URL.Path = "/"
		}
		baseMux.ServeHTTP(w, r)
	}))
	defer ts.Close()

	clientHTTP := &http.Client{
		Transport: &http.Transport{
			DisableCompression: true,
			DialContext: func(_ context.Context, network, _ string) (net.Conn, error) {
				return net.Dial(network, ts.Listener.Addr().String())
			},
		},
	}

	client, err := NewClient(
		WithMode(ServiceToURL),
		WithScheme("http"),
		WithPrefix(prefix),
		WithClient(clientHTTP),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	resp := new(Response)
	err = client.Call(context.Background(), "MathService", "Add", &Request{A: 10, B: 20}, resp)
	if err != nil {
		t.Fatalf("ServiceToURL call failed: %v", err)
	}
	if resp.Sum != 30 {
		t.Fatalf("unexpected result: %d", resp.Sum)
	}
}

func TestNewClient_Validation_EndpointRequiredByDefault(t *testing.T) {
	_, err := NewClient()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestNewClient_Validation_ServiceToHeader_EndpointRequired(t *testing.T) {
	_, err := NewClient(WithMode(ServiceToHeader))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestNewClient_Validation_ServiceToURL_RejectsEndpoint(t *testing.T) {
	_, err := NewClient(
		WithMode(ServiceToURL),
		WithEndpoint("http://127.0.0.1:8080"),
	)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestNewClient_Validation_WithSchemeOnlyForServiceToURL(t *testing.T) {
	_, err := NewClient(
		WithEndpoint("http://127.0.0.1:8080"),
		WithScheme("https"),
	)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestNewClient_Validation_BadSchemeRejected(t *testing.T) {
	_, err := NewClient(
		WithMode(ServiceToURL),
		WithScheme("ftp"),
	)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

/**/

type StreamService struct {
	bytesRead atomic.Int64
}

func (s *StreamService) Download(ctx context.Context, req *Request, w io.Writer) (*Response, error) {
	switch req.A {
	case -1:
		// error before data
		return nil, errors.New("download: simulated error before data")
	case -2:
		// some data then error
		_, _ = io.WriteString(w, "part1|")
		_, _ = io.WriteString(w, "part2|")
		return nil, errors.New("download: simulated error after data")
	}

	// normal
	_, _ = io.WriteString(w, "hello|")
	_, _ = io.WriteString(w, "world|")
	return &Response{Sum: req.A + req.B}, nil
}

func (s *StreamService) Upload(ctx context.Context, req *Request, r io.Reader) (*Response, error) {
	switch req.A {
	case -1:
		return nil, errors.New("upload: simulated error before read")
	case -2:
		// read a bit then error
		buf := make([]byte, 4)
		_, _ = io.ReadFull(r, buf)
		s.bytesRead.Add(int64(len(buf)))
		return nil, errors.New("upload: simulated error after partial read")
	case -3:
		// early return, do not drain the reader, return immediately
		return &Response{Sum: req.A + req.B}, nil
	}

	n, err := io.Copy(io.Discard, r)
	s.bytesRead.Add(n)
	if err != nil {
		return nil, err
	}
	return &Response{Sum: req.A + req.B}, nil
}

func setupStreamServer(t *testing.T) (*StreamService, *httptest.Server) {
	t.Helper()

	svc := new(StreamService)
	h, err := NewHandlerFor[StreamService](svc)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	mux, err := NewMux(h)
	if err != nil {
		t.Fatalf("failed to create mux: %v", err)
	}
	ts := httptest.NewServer(mux)
	return svc, ts
}

func TestServerStream_Success(t *testing.T) {
	_, ts := setupStreamServer(t)
	defer ts.Close()

	c := setupClientToServer(t, ts.URL)

	var dst bytes.Buffer
	resp := new(Response)

	err := c.ServerStream(
		context.Background(),
		"StreamService",
		"Download",
		&Request{A: 10, B: 20},
		&dst,
		resp,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := dst.String(); got != "hello|world|" {
		t.Fatalf("unexpected streamed data: %q", got)
	}
	if resp.Sum != 30 {
		t.Fatalf("unexpected response: %d", resp.Sum)
	}
}

func TestServerStream_ErrorBeforeData_IsHTTP500(t *testing.T) {
	_, ts := setupStreamServer(t)
	defer ts.Close()

	c := setupClientToServer(t, ts.URL)

	var dst bytes.Buffer
	resp := new(Response)

	err := c.ServerStream(
		context.Background(),
		"StreamService",
		"Download",
		&Request{A: -1, B: 0},
		&dst,
		resp,
	)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "download: simulated error before data") {
		t.Fatalf("unexpected error: %v", err)
	}
	if dst.Len() != 0 {
		t.Fatalf("expected no streamed data, got %q", dst.String())
	}
}

func TestServerStream_ErrorAfterData_WithoutFlush_BecomesHTTP500_NoPartialData(t *testing.T) {
	_, ts := setupStreamServer(t)
	defer ts.Close()

	c := setupClientToServer(t, ts.URL)

	var dst bytes.Buffer
	resp := new(Response)

	err := c.ServerStream(
		context.Background(),
		"StreamService",
		"Download",
		&Request{A: -2, B: 0},
		&dst,
		resp,
	)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "download: simulated error after data") {
		t.Fatalf("unexpected error: %v", err)
	}
	if dst.Len() != 0 {
		t.Fatalf("expected no streamed data without flush, got %q", dst.String())
	}
}

type StreamServiceFlushAfterFirst struct{}

func (s *StreamServiceFlushAfterFirst) Download(ctx context.Context, req *Request, w io.Writer) (*Response, error) {
	_, _ = io.WriteString(w, "part1|")
	if f, ok := w.(interface{ Flush() error }); ok {
		_ = f.Flush()
	}
	_, _ = io.WriteString(w, "part2|")
	return nil, errors.New("download: simulated error after data")
}

func setupStreamServerFlushAfterFirst(t *testing.T) *httptest.Server {
	t.Helper()
	svc := new(StreamServiceFlushAfterFirst)
	h, err := NewHandlerFor[StreamServiceFlushAfterFirst](svc)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	mux, err := NewMux(h)
	if err != nil {
		t.Fatalf("failed to create mux: %v", err)
	}
	return httptest.NewServer(mux)
}

func TestServerStream_ErrorAfterData_WithFlush_IsFramedError_AndPartialDataArrives(t *testing.T) {
	ts := setupStreamServerFlushAfterFirst(t)
	defer ts.Close()

	c := setupClientToServer(t, ts.URL)

	var dst bytes.Buffer
	resp := new(Response)

	err := c.ServerStream(
		context.Background(),
		"StreamServiceFlushAfterFirst",
		"Download",
		&Request{A: 1, B: 2},
		&dst,
		resp,
	)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "download: simulated error after data") {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := dst.String(); got != "part1|part2|" {
		t.Fatalf("unexpected streamed data: %q", got)
	}
}

type StreamServiceFlushThenReturn struct{}

func (s *StreamServiceFlushThenReturn) Download(ctx context.Context, req *Request, w io.Writer) (*Response, error) {
	_, _ = io.WriteString(w, "a|")
	if f, ok := w.(interface{ Flush() error }); ok {
		_ = f.Flush()
	}

	_, _ = io.WriteString(w, "b|")
	if f, ok := w.(interface{ Flush() error }); ok {
		_ = f.Flush()
	}

	return &Response{Sum: req.A + req.B}, nil
}

func setupStreamServerFlushThenReturn(t *testing.T) *httptest.Server {
	t.Helper()

	svc := new(StreamServiceFlushThenReturn)
	h, err := NewHandlerFor[StreamServiceFlushThenReturn](svc)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	mux, err := NewMux(h)
	if err != nil {
		t.Fatalf("failed to create mux: %v", err)
	}
	return httptest.NewServer(mux)
}

func TestServerStream_ChunksThenResponse_Success(t *testing.T) {
	ts := setupStreamServerFlushThenReturn(t)
	defer ts.Close()

	c := setupClientToServer(t, ts.URL)

	var dst bytes.Buffer
	resp := new(Response)

	err := c.ServerStream(
		context.Background(),
		"StreamServiceFlushThenReturn",
		"Download",
		&Request{A: 10, B: 20},
		&dst,
		resp,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := dst.String(); got != "a|b|" {
		t.Fatalf("unexpected streamed data: %q", got)
	}
	if resp.Sum != 30 {
		t.Fatalf("unexpected response: %d", resp.Sum)
	}
}

type CancelAwareStreamService struct {
	canceled chan struct{}
}

func (s *CancelAwareStreamService) Download(ctx context.Context, req *Request, w io.Writer) (*Response, error) {
	flush, _ := w.(interface{ Flush() error })
	for {
		select {
		case <-ctx.Done():
			select {
			case <-s.canceled:
			default:
				close(s.canceled)
			}
			return nil, ctx.Err()
		default:
		}

		_, _ = io.WriteString(w, "x")
		if flush != nil {
			_ = flush.Flush()
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func setupCancelAwareServer(t *testing.T) (*CancelAwareStreamService, *httptest.Server) {
	t.Helper()
	svc := &CancelAwareStreamService{canceled: make(chan struct{})}
	h, err := NewHandlerFor[CancelAwareStreamService](svc)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	mux, err := NewMux(h)
	if err != nil {
		t.Fatalf("failed to create mux: %v", err)
	}
	ts := httptest.NewServer(mux)
	return svc, ts
}

func TestServerStream_CancelMidStream_DoesNotHang_AndServerSeesCancel(t *testing.T) {
	svc, ts := setupCancelAwareServer(t)
	defer ts.Close()

	c := setupClientToServer(t, ts.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	var dst bytes.Buffer
	resp := new(Response)

	done := make(chan error, 1)
	go func() {
		done <- c.ServerStream(
			ctx,
			"CancelAwareStreamService",
			"Download",
			&Request{A: 1, B: 2},
			&dst,
			resp,
		)
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error due to ctx cancel, got nil")
		}
		if !strings.Contains(err.Error(), "context deadline exceeded") &&
			!strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
			t.Fatalf("expected ctx error, got: %v", err)
		}
		if dst.Len() == 0 {
			t.Fatalf("expected to receive some streamed bytes before cancel, got empty")
		}

	case <-time.After(2 * time.Second):
		t.Fatal("ServerStream stuck (did not return after ctx cancel)")
	}
	select {
	case <-svc.canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not observe ctx cancellation")
	}
}

func TestClientStream_Success(t *testing.T) {
	svc, ts := setupStreamServer(t)
	defer ts.Close()

	c := setupClientToServer(t, ts.URL)

	size := 128 << 10

	src := strings.NewReader(strings.Repeat("x", size))
	resp := new(Response)

	err := c.ClientStream(
		context.Background(),
		"StreamService",
		"Upload",
		&Request{A: 7, B: 8},
		src,
		resp,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Sum != 15 {
		t.Fatalf("unexpected response: %d", resp.Sum)
	}
	if got := svc.bytesRead.Load(); got != int64(size) {
		t.Fatalf("service read %d bytes, expected %d", got, size)
	}
}

func TestClientStream_ErrorBeforeRead_IsHTTP500(t *testing.T) {
	_, ts := setupStreamServer(t)
	defer ts.Close()

	c := setupClientToServer(t, ts.URL)

	src := strings.NewReader("abcdef")
	resp := new(Response)

	err := c.ClientStream(
		context.Background(),
		"StreamService",
		"Upload",
		&Request{A: -1, B: 0},
		src,
		resp,
	)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "upload: simulated error before read") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestClientStream_ErrorAfterPartialRead_IsHTTP500(t *testing.T) {
	svc, ts := setupStreamServer(t)
	defer ts.Close()

	c := setupClientToServer(t, ts.URL)

	src := strings.NewReader("abcdefghijklmnopqrstuvwxyz")
	resp := new(Response)

	err := c.ClientStream(
		context.Background(),
		"StreamService",
		"Upload",
		&Request{A: -2, B: 0},
		src,
		resp,
	)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "upload: simulated error after partial read") {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := svc.bytesRead.Load(); got < 4 {
		t.Fatalf("expected service to read >= 4 bytes, got %d", got)
	}
}

func TestClientStream_ServerReturnsEarly_DoesNotHang(t *testing.T) {
	_, ts := setupStreamServer(t)
	defer ts.Close()

	c := setupClientToServer(t, ts.URL)

	src := strings.NewReader(strings.Repeat("x", 16<<20))

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	resp := new(Response)

	done := make(chan error, 1)
	go func() {
		done <- c.ClientStream(
			ctx,
			"StreamService",
			"Upload",
			&Request{A: -3, B: 1},
			src,
			resp,
		)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ClientStream stuck")
	}
}

type ctxBlockReader struct{ ctx context.Context }

func (r ctxBlockReader) Read(p []byte) (int, error) {
	<-r.ctx.Done()
	return 0, r.ctx.Err()
}

func TestClientStream_CancelableReader_DoesNotHang(t *testing.T) {
	_, ts := setupStreamServer(t)
	defer ts.Close()

	c := setupClientToServer(t, ts.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	resp := new(Response)

	done := make(chan error, 1)
	go func() {
		done <- c.ClientStream(
			ctx,
			"StreamService",
			"Upload",
			&Request{A: 123, B: 456},
			ctxBlockReader{ctx: ctx},
			resp,
		)
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "context deadline exceeded") &&
			!strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
			t.Fatalf("expected ctx error, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ClientStream stuck (did not return after ctx cancel)")
	}
}

type EarlyReturnUploadService struct{}

func (s *EarlyReturnUploadService) Upload(ctx context.Context, req *Request, r io.Reader) (*Response, error) {
	buf := make([]byte, 1)
	_, _ = r.Read(buf)
	return &Response{Sum: req.A + req.B}, nil
}

func setupEarlyReturnUploadServer(t *testing.T) *httptest.Server {
	t.Helper()

	svc := new(EarlyReturnUploadService)
	h, err := NewHandlerFor[EarlyReturnUploadService](svc)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	mux, err := NewMux(h)
	if err != nil {
		t.Fatalf("failed to create mux: %v", err)
	}
	return httptest.NewServer(mux)
}

type infiniteReader struct{}

func (infiniteReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	return len(p), nil
}

func TestClientStream_ServerReturnsEarly_InfiniteSrc_DoesNotHang(t *testing.T) {
	ts := setupEarlyReturnUploadServer(t)
	defer ts.Close()

	c := setupClientToServer(t, ts.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	resp := new(Response)

	done := make(chan error, 1)
	go func() {
		done <- c.ClientStream(
			ctx,
			"EarlyReturnUploadService",
			"Upload",
			&Request{A: 7, B: 8},
			infiniteReader{},
			resp,
		)
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("expected non-nil error (upload should fail when server returns early), got nil; resp=%+v", resp)
		}
		if !strings.Contains(err.Error(), "error uploading request body") &&
			!errors.Is(err, context.Canceled) &&
			!errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("unexpected error: %v", err)
		}

	case <-time.After(2 * time.Second):
		t.Fatal("ClientStream stuck (server returned early, src is infinite)")
	}
}

func TestHandler_ContentType_WithParams_Accepted(t *testing.T) {
	svc := new(MathService)
	h, err := NewHandlerFor[MathService](svc)
	if err != nil {
		t.Fatal(err)
	}
	codec := jsonCodec{}

	body := new(bytes.Buffer)
	_ = codec.Encode(body, &Request{A: 1, B: 2})

	r := httptest.NewRequest(http.MethodPost, "/MathService/Add", bytes.NewReader(body.Bytes()))
	r.Header.Set("Content-Type", codec.ContentType()+"; charset=utf-8")
	w := httptest.NewRecorder()

	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (err=%q)", w.Code, w.Header().Get(ErrorHeader))
	}
}

func TestHandler_UnsupportedMediaType_415(t *testing.T) {
	svc := new(MathService)
	h, err := NewHandlerFor[MathService](svc)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/MathService/Add", strings.NewReader(`{}`))
	r.Header.Set("Content-Type", "application/does-not-exist")
	w := httptest.NewRecorder()

	h.ServeHTTP(w, r)

	if w.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("expected 415, got %d", w.Code)
	}
}

func TestHandler_MethodNotAllowed(t *testing.T) {
	svc := new(MathService)
	h, err := NewHandlerFor[MathService](svc)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodGet, "/MathService/Add", nil)
	w := httptest.NewRecorder()

	h.ServeHTTP(w, r)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestHandler_ServiceMismatch_404(t *testing.T) {
	svc := new(MathService)
	h, err := NewHandler("OtherService", svc)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/MathService/Add", strings.NewReader(`{}`))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.ServeHTTP(w, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestHandler_MethodWithSlash_404(t *testing.T) {
	svc := new(MathService)
	h, err := NewHandlerFor[MathService](svc)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/MathService/Add/Extra", strings.NewReader(`{}`))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.ServeHTTP(w, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

type MathContract interface {
	Add(context.Context, *Request) (*Response, error)
}

type MathImplWithExtra struct{}

func (MathImplWithExtra) Add(ctx context.Context, req *Request) (*Response, error) {
	return &Response{Sum: req.A + req.B}, nil
}
func (MathImplWithExtra) Extra(ctx context.Context, req *Request) (*Response, error) {
	return &Response{Sum: 999}, nil
}

func TestStrictHandler_ExposesOnlyContractMethods(t *testing.T) {
	h, err := NewStrictHandlerFor[MathContract](MathImplWithExtra{})
	if err != nil {
		t.Fatalf("NewStrictHandlerFor: %v", err)
	}
	r := httptest.NewRequest(http.MethodPost, "/MathContract/Extra", strings.NewReader(`{}`))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for Extra, got %d", w.Code)
	}
}

/**/

type testCodec struct {
	nextID      atomic.Int64
	pingEvents  chan int64
	failOnN     int64
	failOnEqual any
}

func (c *testCodec) ContentType() string            { return "application/test" }
func (c *testCodec) NewDecoder(r io.Reader) Decoder { return nil }
func (c *testCodec) NewEncoder(w io.Writer) Encoder {
	id := c.nextID.Add(1)
	return &testEncoder{
		id:          id,
		w:           w,
		pingEvents:  c.pingEvents,
		failOnN:     c.failOnN,
		failOnEqual: c.failOnEqual,
	}
}
func (c *testCodec) Encode(w io.Writer, v any) error {
	return c.NewEncoder(w).Encode(v)
}
func (c *testCodec) Decode(r io.Reader, v any) error {
	return errors.New("not implemented")
}

type testEncoder struct {
	id int64
	w  io.Writer

	pingEvents chan int64

	failOnN     int64
	failOnEqual any
	calls       int64
}

func (e *testEncoder) Encode(v any) error {
	e.calls++

	// fail on n
	if e.failOnN > 0 && e.calls == e.failOnN {
		return errors.New("test: injected encode failure by call index")
	}

	// fail on value
	if e.failOnEqual != nil && v == e.failOnEqual {
		return errors.New("test: injected encode failure by value")
	}

	// detect ping marker
	if b, ok := v.(byte); ok && b == mPing && e.pingEvents != nil {
		select {
		case e.pingEvents <- e.id:
		default:
		}
	}

	_, _ = e.w.Write([]byte{0xAB})
	return nil
}

type nopWriteCloser struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *nopWriteCloser) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func TestFramedIO_PingLoop_StopsAfterRelease_AndDoesNotWriteToReusedObject(t *testing.T) {
	events := make(chan int64, 128)

	tc := &testCodec{
		pingEvents: events,
	}

	w := &nopWriteCloser{}

	f1 := newFramedIO(tc, w, nil)
	f1.flushed.Store(true)
	f1.activity.Store(time.Now().Add(-10 * time.Second).UnixNano())

	// wait for at least one ping from 1
	var id1 int64
	deadline1 := time.Now().Add(2500 * time.Millisecond)
	for time.Now().Before(deadline1) {
		select {
		case id := <-events:
			id1 = id
			goto gotFirstPing
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatal("did not observe ping for first framedIO instance in time")

gotFirstPing:
	// release instance 1 (should bump generation and stop ping goroutine)
	f1.release()

	// reuse from pool, instance 2 (likely same pointer, but must have a new encoder)
	f2 := newFramedIO(tc, w, nil)
	defer f2.release()

	f2.flushed.Store(true)
	f2.activity.Store(time.Now().Add(-10 * time.Second).UnixNano())

	// collect events for a while; expect to see id2 and never see id1 again after reuse
	var id2 int64
	start := time.Now()
	for time.Since(start) < 2500*time.Millisecond {
		select {
		case id := <-events:
			if id == id1 {
				t.Fatalf("observed ping from old encoder id=%d after release+reuse; ping goroutine likely leaked", id1)
			}
			if id2 == 0 {
				id2 = id
			}
		case <-time.After(10 * time.Millisecond):
		}
	}

	if id2 == 0 {
		t.Fatalf("did not observe any ping from reused framedIO instance; want at least one ping not equal to id1=%d", id1)
	}
}

func TestFramedIO_SendBytes_EncodeFailsOnMarker_ClosesAndFurtherSendsErrClosedPipe(t *testing.T) {
	tc := &testCodec{
		failOnEqual: mBin,
	}
	w := &nopWriteCloser{}
	f := newFramedIO(tc, w, nil)
	defer f.release()

	f.activity.Store(time.Now().UnixNano())

	err := f.sendBytes([]byte("hello"))
	if err == nil {
		t.Fatal("expected error from injected Encode failure, got nil")
	}
	if !f.closed.Load() {
		t.Fatal("expected framedIO.closed=true after Encode failure in sendBytes(marker)")
	}
	if err2 := f.sendMsg("x"); !errors.Is(err2, io.ErrClosedPipe) {
		t.Fatalf("expected io.ErrClosedPipe after closed, got: %v", err2)
	}
}

func TestFramedIO_SendBytes_EncodeFailsOnPayload_ClosesAndFurtherSendsErrClosedPipe(t *testing.T) {
	tc := &testCodec{
		failOnN: 2,
	}
	w := &nopWriteCloser{}
	f := newFramedIO(tc, w, nil)
	defer f.release()

	f.activity.Store(time.Now().UnixNano())

	err := f.sendBytes([]byte("hello"))
	if err == nil {
		t.Fatal("expected error from injected Encode failure on payload, got nil")
	}
	if !f.closed.Load() {
		t.Fatal("expected framedIO.closed=true after Encode failure in sendBytes(payload)")
	}

	if err2 := f.sendBytes([]byte("again")); !errors.Is(err2, io.ErrClosedPipe) {
		t.Fatalf("expected io.ErrClosedPipe after closed, got: %v", err2)
	}
}

func TestFramedIO_SendMsg_EncodeFails_ClosesAndFurtherSendsErrClosedPipe(t *testing.T) {
	tc := &testCodec{
		failOnN: 1,
	}
	w := &nopWriteCloser{}
	f := newFramedIO(tc, w, nil)
	defer f.release()

	f.activity.Store(time.Now().UnixNano())

	err := f.sendMsg("hello")
	if err == nil {
		t.Fatal("expected error from injected Encode failure in sendMsg, got nil")
	}
	if !f.closed.Load() {
		t.Fatal("expected framedIO.closed=true after Encode failure in sendMsg")
	}

	if err2 := f.sendEnd(); !errors.Is(err2, io.ErrClosedPipe) {
		t.Fatalf("expected io.ErrClosedPipe after closed, got: %v", err2)
	}
}

func TestFramedIO_SendEnd_EncodeFails_StillCloses(t *testing.T) {
	tc := &testCodec{
		failOnEqual: mEnd,
	}
	w := &nopWriteCloser{}
	f := newFramedIO(tc, w, nil)
	defer f.release()

	f.activity.Store(time.Now().UnixNano())

	err := f.sendEnd()
	if err == nil {
		t.Fatal("expected error from injected Encode failure in sendEnd, got nil")
	}
	if !f.closed.Load() {
		t.Fatal("expected framedIO.closed=true after sendEnd entered (even if Encode fails)")
	}

	if err2 := f.sendPing(); !errors.Is(err2, io.ErrClosedPipe) {
		t.Fatalf("expected io.ErrClosedPipe after closed, got: %v", err2)
	}
}

func TestFramedIO_SendError_EncodeFails_StillCloses(t *testing.T) {
	tc := &testCodec{
		failOnEqual: mError,
	}
	w := &nopWriteCloser{}
	f := newFramedIO(tc, w, nil)
	defer f.release()

	f.activity.Store(time.Now().UnixNano())

	err := f.sendError(errors.New("boom"))
	if err == nil {
		t.Fatal("expected error from injected Encode failure in sendError, got nil")
	}
	if !f.closed.Load() {
		t.Fatal("expected framedIO.closed=true after sendError entered (even if Encode fails)")
	}
	if err2 := f.sendBytes([]byte("x")); !errors.Is(err2, io.ErrClosedPipe) {
		t.Fatalf("expected io.ErrClosedPipe after closed, got: %v", err2)
	}
}

func TestFramedIO_ConcurrentPingAndWrite_DoesNotDeadlock(t *testing.T) {
	tc := &testCodec{}
	w := &nopWriteCloser{}
	f := newFramedIO(tc, w, nil)
	defer f.release()

	f.flushed.Store(true)
	f.activity.Store(time.Now().Add(-10 * time.Second).UnixNano())

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			_ = f.sendPing()
		}
	}()
	for i := 0; i < 1000; i++ {
		if _, err := f.Write([]byte("abc")); err != nil {
			t.Fatal(err)
		}
	}

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("deadlock suspected between sendPing and Write")
	}
}

func TestFramedIO_NewFramedIO_HasWriteBufWhenWriterProvided(t *testing.T) {
	tc := &testCodec{}
	w := &nopWriteCloser{}
	f := newFramedIO(tc, w, nil)
	defer f.release()

	if f.writeBuf == nil {
		t.Fatal("writeBuf is nil")
	}
	_ = f.flush()
}

func TestFramedIO_PoolReuse_ResetsWriteBufTarget(t *testing.T) {
	tc := &testCodec{}

	w1 := &nopWriteCloser{}
	f1 := newFramedIO(tc, w1, nil)

	_, _ = f1.Write([]byte("x"))
	_ = f1.Flush()
	f1.release()

	w2 := &nopWriteCloser{}
	f2 := newFramedIO(tc, w2, nil)
	defer f2.release()

	_, _ = f2.Write([]byte("y"))
	_ = f2.Flush()

	w2.mu.Lock()
	w2n := w2.buf.Len()
	w2.mu.Unlock()

	if w2n == 0 {
		t.Fatal("expected some bytes written to w2 after pool reuse")
	}
}

func TestFramedIO_NewFramedIO_WithNilWriter_DoesNotStartPingOrPanicOnRelease(t *testing.T) {
	tc := &testCodec{}
	f := newFramedIO(tc, nil, nil)
	f.release()
}

func TestFramedIO_Flush_NoHTTPFlusher_DoesNotPanic(t *testing.T) {
	tc := &testCodec{}
	w := &nopWriteCloser{}
	f := newFramedIO(tc, w, nil)
	defer f.release()

	_, _ = f.writeBuf.Write([]byte("abc"))
	_ = f.flush()
}

func TestFramedIO_WriteAndFlush(t *testing.T) {
	tc := &testCodec{}
	w := &nopWriteCloser{}
	f := newFramedIO(tc, w, nil)
	defer f.release()

	f.activity.Store(time.Now().UnixNano())

	_, err := f.Write(bytes.Repeat([]byte("x"), 1024))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err = f.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestFramedIO_SendBytes_DoesNotFlushAutomatically(t *testing.T) {
	tc := &testCodec{}
	w := &nopWriteCloser{}
	f := newFramedIO(tc, w, nil)
	defer f.release()

	if err := f.sendBytes([]byte("hello")); err != nil {
		t.Fatalf("sendBytes: %v", err)
	}

	w.mu.Lock()
	n := w.buf.Len()
	w.mu.Unlock()

	if n != 0 {
		t.Fatalf("expected 0 bytes in underlying writer before Flush, got %d", n)
	}

	_ = f.flush()

	w.mu.Lock()
	n = w.buf.Len()
	w.mu.Unlock()

	if n == 0 {
		t.Fatal("expected >0 bytes after flush, got 0")
	}
}

type errWriter struct{}

func (errWriter) Write(p []byte) (int, error) { return 0, errors.New("test: write error") }

func TestFramedIO_Flush_ReturnsErrorOnUnderlyingWriteError(t *testing.T) {
	tc := &testCodec{}
	f := newFramedIO(tc, io.Discard, nil)
	defer f.release()

	f.writeBuf.Reset(errWriter{})
	_, _ = f.writeBuf.Write([]byte("abc"))

	if err := f.Flush(); err == nil {
		t.Fatal("expected flush error, got nil")
	}
}

/**/

type RPCMathService struct{}

func (s *RPCMathService) Add(req *Request, resp *Response) error {
	resp.Sum = req.A + req.B
	return nil
}

type StreamBenchService struct{}

func (s *StreamBenchService) Download(ctx context.Context, req *Request, w io.Writer) (*Response, error) {
	if req.A < 0 {
		return nil, errors.New("negative size")
	}
	chunk := req.B
	if chunk <= 0 {
		chunk = 32 << 10
	}
	if chunk > req.A && req.A > 0 {
		chunk = req.A
	}

	buf := make([]byte, chunk)
	remaining := req.A

	for remaining > 0 {
		n := chunk
		if remaining < n {
			n = remaining
		}
		if _, err := w.Write(buf[:n]); err != nil {
			return nil, err
		}
		remaining -= n
	}
	return &Response{Sum: req.A + req.B}, nil
}

func (s *StreamBenchService) DownloadFlushy(ctx context.Context, req *Request, w io.Writer) (*Response, error) {
	if req.A < 0 {
		return nil, errors.New("negative size")
	}
	chunk := req.B
	if chunk <= 0 {
		chunk = 256
	}
	if chunk > req.A && req.A > 0 {
		chunk = req.A
	}

	buf := make([]byte, chunk)
	remaining := req.A

	fl, _ := w.(interface{ Flush() error })

	for remaining > 0 {
		n := chunk
		if remaining < n {
			n = remaining
		}
		if _, err := w.Write(buf[:n]); err != nil {
			return nil, err
		}
		remaining -= n

		if fl != nil {
			if err := fl.Flush(); err != nil {
				return nil, err
			}
		}
	}

	return &Response{Sum: req.A + req.B}, nil
}

func (s *StreamBenchService) Upload(ctx context.Context, req *Request, r io.Reader) (*Response, error) {
	_, err := io.Copy(io.Discard, r)
	if err != nil {
		return nil, err
	}
	return &Response{Sum: req.A + req.B}, nil
}

type benchRW struct {
	hdr http.Header
	dst io.Writer
	buf bytes.Buffer

	status int
}

func (w *benchRW) Reset(dst io.Writer, bufCap int) {
	if w.hdr == nil {
		w.hdr = make(http.Header, 8)
	}
	for k := range w.hdr {
		delete(w.hdr, k)
	}
	w.status = 0
	w.dst = dst
	w.buf.Reset()
	if bufCap > 0 {
		w.buf.Grow(bufCap)
	}
}

func (w *benchRW) Header() http.Header  { return w.hdr }
func (w *benchRW) WriteHeader(code int) { w.status = code }
func (w *benchRW) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	if w.dst != nil {
		return w.dst.Write(p)
	}
	return w.buf.Write(p)
}
func (w *benchRW) Flush() {}

type inprocTransport struct {
	h  http.Handler
	rw *benchRW
	mu sync.Mutex
}

func (t *inprocTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.rw.Reset(nil, int(req.ContentLength)+256)
	t.h.ServeHTTP(t.rw, req)

	if t.rw.status == 0 {
		t.rw.status = http.StatusOK
	}

	body := append([]byte(nil), t.rw.buf.Bytes()...)
	return &http.Response{
		StatusCode:    t.rw.status,
		Header:        t.rw.hdr.Clone(),
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       req,
	}, nil
}

func setupVRPCHTTPUnary(b *testing.B) (*httptest.Server, *Client) {
	b.Helper()
	ts := httptest.NewServer(Def[MathService](new(MathService)))
	c, err := NewClient(WithEndpoint(ts.URL))
	if err != nil {
		ts.Close()
		b.Fatal(err)
	}
	return ts, c
}

func setupNetRPCHTTPUnary(b *testing.B) (*httptest.Server, *rpc.Client) {
	b.Helper()

	srv := rpc.NewServer()
	if err := srv.RegisterName("MathService", new(RPCMathService)); err != nil {
		b.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, srv)

	ts := httptest.NewServer(mux)

	cli, err := rpc.DialHTTP("tcp", ts.Listener.Addr().String())
	if err != nil {
		ts.Close()
		b.Fatal(err)
	}
	return ts, cli
}

func setupVRPCDirectUnary(b *testing.B) *Client {
	b.Helper()

	c, err := NewClient(
		WithClient(&http.Client{
			Transport: &inprocTransport{
				h:  Def[MathService](new(MathService)),
				rw: &benchRW{},
			},
		}),
		WithEndpoint("http://inproc"), // ignored
	)
	if err != nil {
		b.Fatal(err)
	}
	return c
}

func setupNetRPCDirectPipe(b *testing.B) (*rpc.Client, func()) {
	b.Helper()

	serverConn, clientConn := net.Pipe()

	srv := rpc.NewServer()
	if err := srv.RegisterName("MathService", new(RPCMathService)); err != nil {
		_ = serverConn.Close()
		_ = clientConn.Close()
		b.Fatal(err)
	}

	go srv.ServeConn(serverConn)
	cli := rpc.NewClient(clientConn)

	closeFn := func() {
		_ = cli.Close()
		_ = serverConn.Close()
	}
	return cli, closeFn
}

func setupVRPCStreamHTTP(b *testing.B) (*httptest.Server, *Client) {
	b.Helper()
	ts := httptest.NewServer(Def[StreamBenchService](new(StreamBenchService)))
	c, err := NewClient(WithEndpoint(ts.URL))
	if err != nil {
		ts.Close()
		b.Fatal(err)
	}
	return ts, c
}

func BenchmarkVRPC_Call_HTTP(b *testing.B) {
	ts, c := setupVRPCHTTPUnary(b)
	defer ts.Close()

	req := &Request{A: 1, B: 2}
	res := new(Response)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		*res = Response{}
		if err := c.Call(context.Background(), "MathService", "Add", req, res); err != nil {
			b.Fatal(err)
		}
		if res.Sum != 3 {
			b.Fatalf("bad sum: %d", res.Sum)
		}
	}
}

func BenchmarkNetRPC_Call_HTTP(b *testing.B) {
	ts, c := setupNetRPCHTTPUnary(b)
	defer ts.Close()
	defer c.Close()

	req := &Request{A: 1, B: 2}
	res := new(Response)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		*res = Response{}
		if err := c.Call("MathService.Add", req, res); err != nil {
			b.Fatal(err)
		}
		if res.Sum != 3 {
			b.Fatalf("bad sum: %d", res.Sum)
		}
	}
}

func BenchmarkVRPC_Call_InProc(b *testing.B) {
	c := setupVRPCDirectUnary(b)

	req := &Request{A: 1, B: 2}
	res := new(Response)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		*res = Response{}
		if err := c.Call(context.Background(), "MathService", "Add", req, res); err != nil {
			b.Fatal(err)
		}
		if res.Sum != 3 {
			b.Fatalf("bad sum: %d", res.Sum)
		}
	}
}

func BenchmarkNetRPC_Call_InProc(b *testing.B) {
	c, closeFn := setupNetRPCDirectPipe(b)
	defer closeFn()

	req := &Request{A: 1, B: 2}
	res := new(Response)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		*res = Response{}
		if err := c.Call("MathService.Add", req, res); err != nil {
			b.Fatal(err)
		}
		if res.Sum != 3 {
			b.Fatalf("bad sum: %d", res.Sum)
		}
	}
}

func BenchmarkVRPC_ServerStream_HTTP_8MiB(b *testing.B) {
	ts, c := setupVRPCStreamHTTP(b)
	defer ts.Close()

	ctx := context.Background()
	req := &Request{A: 8 << 20, B: 32 << 10} // total bytes, chunk size
	resp := new(Response)
	dst := new(bytes.Buffer)

	b.SetBytes(int64(req.A))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		dst.Reset()
		*resp = Response{}

		if err := c.ServerStream(ctx, "StreamBenchService", "Download", req, dst, resp); err != nil {
			b.Fatal(err)
		}
		if dst.Len() != req.A {
			b.Fatalf("got %d bytes, want %d", dst.Len(), req.A)
		}
	}
}

func BenchmarkVRPC_ServerStream_InProc_8MiB(b *testing.B) {
	svc := new(StreamBenchService)
	h := Def[StreamBenchService](svc)
	codec := msgpackCodec{}

	var reqBuf bytes.Buffer
	req := &Request{A: 8 << 20, B: 32 << 10}
	if err := codec.Encode(&reqBuf, req); err != nil {
		b.Fatal(err)
	}
	reqBytes := reqBuf.Bytes()

	bb := make([]byte, 0, 1<<10)

	b.SetBytes(int64(req.A))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pr, pw := io.Pipe()

		go func() {
			r := &http.Request{
				Method: http.MethodPost,
				Header: http.Header{"Content-Type": []string{codec.ContentType()}},
				URL:    &url.URL{Path: "/StreamBenchService/Download"},
				Body:   io.NopCloser(bytes.NewReader(reqBytes)),
			}
			rw := &benchRW{}
			rw.Reset(pw, 0)
			h.ServeHTTP(rw, r)
			_ = pw.Close()
		}()

		in := newFramedIO(codec, nil, pr)
		total := 0

		for {
			m, err := in.decodeMarker()
			if err != nil {
				b.Fatal(err)
			}
			switch m {
			case mPing:
				continue
			case mBin:
				bb = bb[:0]
				if err = in.dec.Decode(&bb); err != nil {
					b.Fatal(err)
				}
				total += len(bb)
			case mMsg:
				var r Response
				if err = in.dec.Decode(&r); err != nil {
					b.Fatal(err)
				}
			case mEnd:
				in.release()
				goto done
			case mError:
				s, _ := in.decodeString()
				b.Fatal(s)
			default:
				b.Fatal("unknown marker")
			}
		}
	done:
		if total != req.A {
			b.Fatalf("got %d want %d", total, req.A)
		}
	}
}

func BenchmarkVRPC_ServerStream_HTTP_Flush_512KiB_256B(b *testing.B) {
	ts, c := setupVRPCStreamHTTP(b)
	defer ts.Close()

	ctx := context.Background()
	req := &Request{A: 512 << 10, B: 256} // small chunks + flush in service
	resp := new(Response)
	dst := new(bytes.Buffer)

	b.SetBytes(int64(req.A))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		dst.Reset()
		*resp = Response{}

		if err := c.ServerStream(ctx, "StreamBenchService", "DownloadFlushy", req, dst, resp); err != nil {
			b.Fatal(err)
		}
		if dst.Len() != req.A {
			b.Fatalf("got %d bytes, want %d", dst.Len(), req.A)
		}
	}
}

func BenchmarkVRPC_ServerStream_InProc_Flush_512KiB_256B(b *testing.B) {
	svc := new(StreamBenchService)
	h := Def[StreamBenchService](svc)
	codec := msgpackCodec{}

	var reqBuf bytes.Buffer
	req := &Request{A: 512 << 10, B: 256}
	if err := codec.Encode(&reqBuf, req); err != nil {
		b.Fatal(err)
	}
	reqBytes := reqBuf.Bytes()

	bb := make([]byte, 0, 1<<10)

	b.SetBytes(int64(req.A))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pr, pw := io.Pipe()

		go func() {
			r := &http.Request{
				Method: http.MethodPost,
				Header: http.Header{"Content-Type": []string{codec.ContentType()}},
				URL:    &url.URL{Path: "/StreamBenchService/DownloadFlushy"},
				Body:   io.NopCloser(bytes.NewReader(reqBytes)),
			}
			rw := &benchRW{}
			rw.Reset(pw, 0)
			h.ServeHTTP(rw, r)
			_ = pw.Close()
		}()

		in := newFramedIO(codec, nil, pr)
		total := 0

		for {
			m, err := in.decodeMarker()
			if err != nil {
				b.Fatal(err)
			}
			switch m {
			case mPing:
				continue
			case mBin:
				bb = bb[:0]
				if err := in.dec.Decode(&bb); err != nil {
					b.Fatal(err)
				}
				total += len(bb)
			case mMsg:
				var r Response
				if err := in.dec.Decode(&r); err != nil {
					b.Fatal(err)
				}
			case mEnd:
				in.release()
				goto done
			case mError:
				s, _ := in.decodeString()
				b.Fatal(s)
			default:
				b.Fatal("unknown marker")
			}
		}
	done:
		if total != req.A {
			b.Fatalf("got %d want %d", total, req.A)
		}
	}
}

func BenchmarkVRPC_ClientStream_HTTP_8MiB(b *testing.B) {
	ts, c := setupVRPCStreamHTTP(b)
	defer ts.Close()

	ctx := context.Background()
	req := &Request{A: 1, B: 2}
	resp := new(Response)

	payload := strings.Repeat("x", 8<<20)

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		*resp = Response{}
		src := strings.NewReader(payload)
		if err := c.ClientStream(ctx, "StreamBenchService", "Upload", req, src, resp); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkVRPC_ClientStream_InProc_8MiB(b *testing.B) {
	svc := new(StreamBenchService)
	h := Def[StreamBenchService](svc)
	codec := msgpackCodec{}

	ctx := context.Background()
	reqObj := &Request{A: 1, B: 2}
	resp := new(Response)

	payload := bytes.Repeat([]byte{'x'}, 8<<20)

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		*resp = Response{}

		pr, pw := io.Pipe()

		errCh := make(chan error, 1)
		go func() {
			defer close(errCh)
			out := newFramedIO(codec, pw, nil)
			defer out.release()
			defer func() { _ = pw.Close() }()

			if err := out.sendMsg(reqObj); err != nil {
				_ = pw.CloseWithError(err)
				errCh <- err
				return
			}
			if _, err := io.Copy(out, bytes.NewReader(payload)); err != nil {
				_ = out.sendError(err)
				_ = pw.CloseWithError(err)
				errCh <- err
				return
			}
			if err := out.sendEnd(); err != nil {
				_ = pw.CloseWithError(err)
				errCh <- err
				return
			}
			errCh <- nil
		}()

		r := &http.Request{
			Method: http.MethodPost,
			Header: http.Header{"Content-Type": []string{codec.ContentType()}},
			URL:    &url.URL{Path: "/StreamBenchService/Upload"},
			Body:   pr,
		}
		r = r.WithContext(ctx)
		r.ContentLength = -1

		rw := &benchRW{}
		rw.Reset(nil, 512)
		h.ServeHTTP(rw, r)

		if rw.status != http.StatusOK {
			errText := rw.hdr.Get(ErrorHeader)
			b.Fatalf("status %d err=%q", rw.status, errText)
		}
		if err := codec.Decode(bytes.NewReader(rw.buf.Bytes()), resp); err != nil {
			b.Fatal(err)
		}
		if err := <-errCh; err != nil {
			b.Fatal(err)
		}
	}
}

type chunkedReader struct {
	b    []byte
	pos  int
	size int
}

func (r *chunkedReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.b) {
		return 0, io.EOF
	}
	n := r.size
	if n <= 0 {
		n = 256
	}
	if n > len(p) {
		n = len(p)
	}
	rem := len(r.b) - r.pos
	if rem < n {
		n = rem
	}
	copy(p[:n], r.b[r.pos:r.pos+n])
	r.pos += n
	return n, nil
}

func BenchmarkVRPC_ClientStream_HTTP_Flush_512KiB_256B(b *testing.B) {
	ts, c := setupVRPCStreamHTTP(b)
	defer ts.Close()

	ctx := context.Background()
	req := &Request{A: 1, B: 2}
	resp := new(Response)

	payload := bytes.Repeat([]byte{'x'}, 512<<10)

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		src := &chunkedReader{b: payload, size: 256}
		*resp = Response{}
		if err := c.ClientStream(ctx, "StreamBenchService", "Upload", req, src, resp); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkVRPC_ClientStream_InProc_Flush_512KiB_256B(b *testing.B) {

	h := Def[StreamBenchService](new(StreamBenchService))
	codec := msgpackCodec{}

	ctx := context.Background()
	reqObj := &Request{A: 1, B: 2}
	resp := new(Response)

	payload := bytes.Repeat([]byte{'x'}, 512<<10)

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		*resp = Response{}

		pr, pw := io.Pipe()

		errCh := make(chan error, 1)
		go func() {
			defer close(errCh)
			out := newFramedIO(codec, pw, nil)
			defer out.release()
			defer func() { _ = pw.Close() }()

			if err := out.sendMsg(reqObj); err != nil {
				_ = pw.CloseWithError(err)
				errCh <- err
				return
			}
			src := &chunkedReader{b: payload, size: 256}
			if _, err := io.Copy(out, src); err != nil {
				_ = out.sendError(err)
				_ = pw.CloseWithError(err)
				errCh <- err
				return
			}
			if err := out.sendEnd(); err != nil {
				_ = pw.CloseWithError(err)
				errCh <- err
				return
			}
			errCh <- nil
		}()

		r := &http.Request{
			Method: http.MethodPost,
			Header: http.Header{"Content-Type": []string{codec.ContentType()}},
			URL:    &url.URL{Path: "/StreamBenchService/Upload"},
			Body:   pr,
		}
		r = r.WithContext(ctx)
		r.ContentLength = -1

		rw := &benchRW{}
		rw.Reset(nil, 512)
		h.ServeHTTP(rw, r)

		if rw.status != http.StatusOK {
			errText := rw.hdr.Get(ErrorHeader)
			b.Fatalf("status %d err=%q", rw.status, errText)
		}
		if err := codec.Decode(bytes.NewReader(rw.buf.Bytes()), resp); err != nil {
			b.Fatal(err)
		}
		if err := <-errCh; err != nil {
			b.Fatal(err)
		}
	}
}
