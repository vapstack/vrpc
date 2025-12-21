package vrpc

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/rpc"
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

func TestServiceToHeader_SetsHostHeader(t *testing.T) {
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

func TestServiceToURL_WithPrefix(t *testing.T) {
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

func BenchmarkVRPC_Call_HTTP(b *testing.B) {
	svc := new(MathService)
	handler := Def[MathService](svc)

	mux, err := NewMux(handler)
	if err != nil {
		b.Fatal(err)
	}
	server := httptest.NewServer(mux)
	defer server.Close()

	client, err := NewClient(WithEndpoint(server.URL))
	if err != nil {
		b.Fatal(err)
	}

	req := &Request{A: 1, B: 2}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		res := new(Response)
		err = client.Call(ctx, "MathService", "Add", req, res)
		// _, err = CallFor[MathService, Response](client, ctx, "Add", req)
		if err != nil {
			b.Fatal(err)
		}
	}
}

type RPCMathService struct{}

func (s *RPCMathService) Add(req *Request, resp *Response) error {
	resp.Sum = req.A + req.B
	return nil
}

func setupNetRPCHTTP(b *testing.B) (addr string, closeFn func()) {
	b.Helper()

	srv := rpc.NewServer()
	if err := srv.RegisterName("MathService", new(RPCMathService)); err != nil {
		b.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, srv)
	mux.Handle(rpc.DefaultDebugPath, srv)

	ts := httptest.NewServer(mux)
	return ts.Listener.Addr().String(), ts.Close
}

func BenchmarkNetRPC_Call_HTTP(b *testing.B) {
	addr, closeServer := setupNetRPCHTTP(b)
	defer closeServer()

	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	req := &Request{A: 1, B: 2}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		res := new(Response)
		if err = client.Call("MathService.Add", req, res); err != nil {
			b.Fatal(err)
		}
		if res.Sum != 3 {
			b.Fatalf("bad sum: %d", res.Sum)
		}
	}
}

func BenchmarkVRPC_Call_Direct(b *testing.B) {
	svc := new(MathService)
	h := Def[MathService](svc)

	codec := msgpackCodec{}

	buf := new(bytes.Buffer)
	if err := codec.Encode(buf, &Request{A: 1, B: 2}); err != nil {
		b.Fatal(err)
	}
	body := buf.Bytes()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r := httptest.NewRequest(http.MethodPost, "/MathService/Add", bytes.NewReader(body))
		r = r.WithContext(context.Background())
		r.Header.Set("Content-Type", codec.ContentType())

		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)

		if w.Code != http.StatusOK {
			b.Fatalf("status: %d", w.Code)
		}
		if err := w.Header().Get(ErrorHeader); err != "" {
			b.Fatalf("err header: %s", err)
		}
	}
}

func dialRPC(b *testing.B) (*rpc.Client, func()) {
	b.Helper()

	serverConn, clientConn := net.Pipe()

	srv := rpc.NewServer()
	if err := srv.RegisterName("MathService", new(RPCMathService)); err != nil {
		_ = serverConn.Close()
		_ = clientConn.Close()
		b.Fatal(err)
	}

	go srv.ServeConn(serverConn)

	c := rpc.NewClient(clientConn)

	return c, func() {
		_ = c.Close()
		_ = serverConn.Close()
	}
}

func BenchmarkNetRPC_Call_Direct(b *testing.B) {
	client, closeFn := dialRPC(b)
	defer closeFn()

	req := &Request{A: 1, B: 2}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		res := new(Response)
		if err := client.Call("MathService.Add", req, &res); err != nil {
			b.Fatal(err)
		}
		if res.Sum != 3 {
			b.Fatalf("bad sum: %d", res.Sum)
		}
	}
}
