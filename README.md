# vrpc

[![GoDoc](https://godoc.org/github.com/vapstack/vrpc?status.svg)](https://godoc.org/github.com/vapstack/vrpc)
[![License](https://img.shields.io/badge/license-Apache2-blue.svg)](https://raw.githubusercontent.com/vapstack/vrpc/master/LICENSE)

RPC framework for Go running over standard HTTP.

- No code generation.
- Built on top of standard `net/http` without manual connection management.
- Compatible with standard proxies, balancers and service mesh environments.
- Contracts are defined using standard Go structs and methods.
- Service name can be derived from the generic type argument or from implementation type.
- Supports synchronous calls, asynchronous notifications and streaming from any side.
- Pluggable codecs (selected by `Content-Type`).\
  JSON, gob and [msgpack](https://github.com/vmihailenco/msgpack) are supported out of the box.
- Client uses msgpack by default.
- Server falls back to JSON when `Content-Type` is missing.

### Why?

To combine the simplicity of `net/rpc` with the infrastructure compatibility of standard HTTP.

#### Comparison

| Feature           | vrpc              | gRPC              | net/rpc         | REST              |
|-------------------|-------------------|-------------------|-----------------|-------------------|
| **Transport**     | HTTP/1.1+         | HTTP/2            | Custom TCP/HTTP | HTTP/1.1+         |
| **Contracts**     | Go Structs        | Protobuf          | Go Structs      | OpenAPI / Swagger |
| **Code Gen**      | None              | Required          | None            | Optional          |
| **Semantics**     | RPC (Actions)     | RPC (Actions)     | RPC (Actions)   | Resources (CRUD)  |
| **Context**       | `context.Context` | `context.Context` | Limited         | `context.Context` |
| **Performance**   | Moderate          | High              | High            | Varies            |
| **Compatibility** | High              | Moderate          | Low             | High              |
| **Streaming**     | One-way           | Bidi              | No              | Varies            |

### Usage

Services must implement methods with the following signature:

```go
// unary
func (s *Service) Method(ctx context.Context, req *Request) (*Response, error)

// server streaming
func (s *Service) Method(ctx context.Context, req *Request, w io.Writer) (*Response, error)

// client streaming
func (s *Service) Method(ctx context.Context, req *Request, r io.Reader) (*Response, error)
```

The request parameter may be either a value or a pointer.

Bidirectional streaming is hard to get right on top of HTTP/1 so it's not supported.

### Server

```go

type HelloRequest struct{ Name string }
type HelloResponse struct{ Greeting string }

type GreeterService struct{}

func (s *GreeterService) SayHello(ctx context.Context, req *HelloRequest) (*HelloResponse, error) {
    return &HelloResponse{Greeting: "Hello, " + req.Name}, nil
}

func main() {
	
    s := new(GreeterService)
    
    handler, err := vrpc.NewHandler("GreeterService", s)
    // or
    handler, err := vrpc.NewHandlerOf(s)
    // or
    handler, err := vrpc.NewHandlerFor[GreeterService](s)
    
    
    http.ListenAndServe(":8080", handler)
    // or
    mux, err := vrpc.NewMux(handler1, handler2, handler3)
    http.ListenAndServe(":8080", mux)
}
```

### Client

```go
func main() {

    c, err := vrpc.NewClient(vrpc.WithEndpoint("http://localhost:8080"))

    req := &HelloRequest{Name: "World"}

    // standard call

    resp := new(HelloResponse)
    err := c.Call(ctx, "GreeterService", "SayHello", req, resp)
	
    // or with a generic helper
    resp, err := vrpc.Call[HelloResponse](c, ctx, "GreeterService", "SayHello", req)
	
    // or with another generic helper
    resp, err := vrpc.CallFor[GreeterService, HelloResponse](c, ctx, "SayHello", req)

    // notify - does not wait for a server to process the request
    err = c.Notify(ctx, "GreeterService", "SayHello", req)

    // beacon - sends in the background, ignoring all but encoding errors
    err = c.Beacon(ctx, "GreeterService", "SayHello", req)
}
```

### Server Streaming

```go
func (s *Service) Method(context.Context, *Request, io.Writer) (*Response, error)
```
The data written to `io.Writer` is streamed to the client incrementally.
After the method returns, the returned `*Response` is sent as the final message 
or, if an error is returned, the error is sent to the client.

`io.Writer` provided to the method also implements
```go
interface { Flush() error }
```

Calling `Flush` forces buffered data to be sent immediately.
Otherwise, data may be buffered to improve throughput.
With frequent `Flush`, latency is minimized at the cost of throughput.
The choice is left to the service implementation.

On the client side, server streaming is consumed using:
```go
err := client.ServerStream(ctx, "ServiceName", "Method", req, dst, resp)
```
`dst` receives streamed binary data, `resp` receives the final response.

### Client Streaming

```go
func (s *Service) Method(context.Context, *Request, io.Reader) (*Response, error)
```

Client uploads a stream of binary data, which is exposed to the service as `io.Reader`.
The initial request object is decoded before streaming begins.
The service may read from `io.Reader` incrementally.
When the client finishes uploading, the service returns a response or an error.

On the client side, client streaming is performed using:
```
err := client.ClientStream(ctx, "ServiceName", "Method", req, src, resp)
```
`src` is read until EOF, `resp` receives the final response.

If the server returns before fully reading the stream, the client upload is aborted.

Context cancellation should be handled and respected by both sides.

### Service Mesh Environments

```go
// requests will be made to the provided endpoint
// with the Host header set to a service name (e.g. GreeterService)
c, err := vrpc.NewClient(
    vrpc.WithEndpoint("http://localhost:8080"), 
    vrpc.WithMode(vrpc.ServiceToHeader))


// requests will be made to the URL "<scheme>://<ServiceName>/<ServiceName>/<Method>"
// with the Host header set to a service name (e.g. GreeterService)
c, err := vrpc.NewClient(
    vrpc.WithMode(vrpc.ServiceToURL))
```

### Client Options

```go
func WithEndpoint(endpoint string) ClientOption
func WithClient(c *http.Client) ClientOption
func WithCodec(codec Codec) ClientOption
func WithMode(mode Mode) ClientOption
func WithScheme(scheme string) ClientOption
func WithPrefix(prefix string) ClientOption
```

### Codecs

```go

type Codec interface {
    Encode(w io.Writer, v any) error
    Decode(r io.Reader, v any) error
    ContentType() string
}

vrpc.RegisterCodec(c)
```

JSON, gob and msgpack are already included.

### Interface Types

```go
type Math struct {
    // ...
}

type MathService interface {
    Sum(context.Context, *SumRequest) (*SumResponse, error)
}

func main() {
    impl := new(Math)

    // service name is "CustomName"
    h, err := NewHandler("CustomName", impl)

    // service name is "Math"
    h, err := NewHandlerOf(impl)

    // service name is "MathService",
    // the handler exposes all suitable methods of the concrete implementation
    h, err := NewHandlerFor[MathService](impl)

    // service name is "MathService",
    // the handler only exposes suitable methods of the MathService interface
    h, err := NewStrictHandlerFor[MathService](impl)
    
}
```

### Benchmarks

`vrpc` vs `net/rpc`

```
cpu: AMD Ryzen 9 5900HX with Radeon Graphics
BenchmarkVRPC_Call_HTTP-16          13016     90857 ns/op     15604 B/op    86 allocs/op
BenchmarkNetRPC_Call_HTTP-16        25706     46758 ns/op       496 B/op    15 allocs/op
BenchmarkVRPC_Call_InProc-16       205393      5643 ns/op     10944 B/op    31 allocs/op
BenchmarkNetRPC_Call_InProc-16     156754      7677 ns/op       514 B/op    16 allocs/op
```
It's almost twice as slow as `net/rpc`. Most allocations come from `net/http`.

Running the same benchmarks with a [fasthttp](https://github.com/valyala/fasthttp) 
backend (not published here) yields better results:

```
cpu: AMD Ryzen 9 5900HX with Radeon Graphics
BenchmarkVRPC_Call_HTTP-16          24279     42743 ns/op       334 B/op    13 allocs/op
BenchmarkNetRPC_Call_HTTP-16        25471     48041 ns/op       496 B/op    15 allocs/op
BenchmarkVRPC_Call_Direct-16       391231      2854 ns/op       489 B/op    22 allocs/op
BenchmarkNetRPC_Call_Direct-16     152834      7723 ns/op       513 B/op    16 allocs/op
```

However, this comes at the cost of breaking compatibility with the standard library.\
After weighing the trade-offs, I decided to stick with `net/http`.

### Internals

- **URL**:
    - **POST** `<endpoint>/<ServiceName>/<MethodName>`
    - **POST** `<scheme>://<ServiceName>/<ServiceName>/<MethodName>` with `ServiceToURL`
- **Headers**:
    - `Content-Type` - from the codec (`application/json`, `application/gob`, etc.)
    - `X-Vrpc-Err` - error message when the call failed
    - `X-Vrpc` - call mode
- **Streaming**:
    - Streaming is implemented on top of HTTP using a framed protocol.
    - Frame types:
        - `1` - message (request or response value)
        - `2` - binary (streamed data)
        - `3` - ping (keepalive)
        - `4` - error (end of stream)
        - `0` - final (end of stream)
    - The framing format is internal and not part of the user API,
      but allows custom implementations in other languages.
      All frames are encoded using the same (requested or default) codec
      as the request. Frame marker is a simple `byte` also encoded by the codec.
      Message (`1`) and binary (`2`) markers are followed by the encoded data.
    - Keepalive (pings):
        - Ping frames are sent only when there is no recent activity.
        - Ping frames prevent idle connections from being closed by proxies.
        - Ping traffic is suppressed when the stream is actively used.
        - Receivers always ignore ping frames.
        - Sending policy is implementation-defined.
    - Streaming writers are not safe for concurrent use.
    - Streaming output is buffered:
        - Writes without `Flush` prioritize throughput.
        - Explicit `Flush` prioritizes latency.
        - HTTP transport may also flush automatically based on buffer size.
    - Streaming using JSON as a codec is inefficient.
