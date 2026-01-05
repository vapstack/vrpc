package vrpc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"net/url"
	"path"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

const (
	// ErrorHeader is the HTTP header key used to report error messages from the server.
	ErrorHeader = "X-Vrpc-Err"
	// ProtoHeader is the HTTP header key used to specify the RPC call mode.
	ProtoHeader = "X-Vrpc"

	// StatusEncodingError is returned by server when it failed to encode the response.
	StatusEncodingError = 567
)

var (
	codecs = map[string]Codec{
		gobCodec{}.ContentType():     gobCodec{},
		jsonCodec{}.ContentType():    jsonCodec{},
		msgpackCodec{}.ContentType(): msgpackCodec{},
	}

	bufferPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}
	argPool    = sync.Pool{New: func() any {
		args := make([]reflect.Value, 3)
		return &args
	}}

	contextType = reflect.TypeOf((*context.Context)(nil)).Elem()
	errorType   = reflect.TypeOf((*error)(nil)).Elem()
	readerType  = reflect.TypeOf((*io.Reader)(nil)).Elem()
	writerType  = reflect.TypeOf((*io.Writer)(nil)).Elem()
)

// RegisterCodec adds a codec to the global list of server codecs.
// It is not safe for concurrent use and must be called before any Handler starts serving requests.
func RegisterCodec(codec Codec) {
	codecs[codec.ContentType()] = codec
}

// Error type is returned for RPC errors (transport, encoding, decoding).
// Errors returned by the service implementation are not wrapped by this type.
type Error struct{ msg string }

func (e *Error) Error() string { return e.msg }

var (
	ErrNotFound = &Error{"not found"}
	ErrNoCodec  = &Error{"codec not supported"}
)

// Codec defines the interface for encoding and decoding RPC messages.
type Codec interface {
	Encode(w io.Writer, v any) error
	Decode(r io.Reader, v any) error
	NewEncoder(w io.Writer) Encoder
	NewDecoder(r io.Reader) Decoder
	ContentType() string
}

type (
	Encoder interface{ Encode(any) error }
	Decoder interface{ Decode(any) error }
)

type msgpackCodec struct{}

func (msgpackCodec) NewEncoder(w io.Writer) Encoder { return msgpack.NewEncoder(w) }
func (msgpackCodec) NewDecoder(r io.Reader) Decoder { return msgpack.NewDecoder(r) }

func (msgpackCodec) Encode(w io.Writer, v any) error {
	enc := msgpackEncPool.Get().(*msgpack.Encoder)
	enc.Reset(w)
	err := enc.Encode(v)
	msgpackEncPool.Put(enc)
	return err
}

func (msgpackCodec) Decode(r io.Reader, v any) error {
	dec := msgpackDecPool.Get().(*msgpack.Decoder)
	dec.Reset(r)
	err := dec.Decode(v)
	msgpackDecPool.Put(dec)
	return err
}

func (msgpackCodec) ContentType() string { return "application/msgpack" }

var msgpackEncPool = sync.Pool{New: func() any { return msgpack.NewEncoder(io.Discard) }}
var msgpackDecPool = sync.Pool{New: func() any { return msgpack.NewDecoder(strings.NewReader("")) }}

type gobCodec struct{}

func (gobCodec) NewEncoder(w io.Writer) Encoder  { return gob.NewEncoder(w) }
func (gobCodec) NewDecoder(r io.Reader) Decoder  { return gob.NewDecoder(r) }
func (gobCodec) Encode(w io.Writer, v any) error { return gob.NewEncoder(w).Encode(v) }
func (gobCodec) Decode(r io.Reader, v any) error { return gob.NewDecoder(r).Decode(v) }
func (gobCodec) ContentType() string             { return "application/gob" }

type jsonCodec struct{}

func (jsonCodec) NewEncoder(w io.Writer) Encoder  { return json.NewEncoder(w) }
func (jsonCodec) NewDecoder(r io.Reader) Decoder  { return json.NewDecoder(r) }
func (jsonCodec) Encode(w io.Writer, v any) error { return json.NewEncoder(w).Encode(v) }
func (jsonCodec) Decode(r io.Reader, v any) error { return json.NewDecoder(r).Decode(v) }
func (jsonCodec) ContentType() string             { return "application/json" }

var defaultCodec = msgpackCodec{}

/**/

type methodType struct {
	mode methodMode

	fn reflect.Value
	rt reflect.Type
}

func (m *methodType) call(args *[]reflect.Value, n int) (results []reflect.Value, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("service panic: %v", p)
		}
		clear(*args)
		argPool.Put(args)
	}()
	return m.fn.Call((*args)[:n]), nil
}

type methodMode uint8

const (
	modeUnary        methodMode = iota // Method(context.Context, *Request) (*Response, error)
	modeServerStream                   // Method(context.Context, *Request, io.Writer) (*Response,error)
	modeClientStream                   // Method(context.Context, *Request, io.Reader) (*Response,error)
)

// Handler serves RPC requests for a specific service implementation.
type Handler struct {
	name    string
	impl    reflect.Value
	methods map[string]*methodType
}

// NewHandler creates a new Handler for the given implementation, using a user-provided service name.
// It reflects over impl to find suitable methods.
// A suitable method must have one of the following signatures:
//
//	Method(context.Context, *Request) (*Response, error). // unary
//	Method(context.Context, *Request, io.Writer) (*Response, error)  // download/stream
//	Method(context.Context, *Request, io.Reader) (*Response, error)  // upload/stream
func NewHandler(service string, impl any) (*Handler, error) {
	if strings.TrimSpace(service) == "" {
		return nil, fmt.Errorf("service name must not be empty")
	}
	if impl == nil {
		return nil, fmt.Errorf("impl must not be nil")
	}
	return newHandler(service, impl, nil, false)
}

// NewHandlerOf creates a new Handler for the given implementation,
// inferring the service name from the implementation type.
func NewHandlerOf(impl any) (*Handler, error) {
	if impl == nil {
		return nil, fmt.Errorf("impl must not be nil")
	}
	n, err := serviceName(reflect.TypeOf(impl))
	if err != nil {
		return nil, err
	}
	return newHandler(n, impl, nil, false)
}

// NewHandlerFor creates a new Handler for the given implementation,
// using type T to determine the service name.
//
// If T is an interface, impl must implement it.
// However, even when T is an interface, all suitable methods found on the concrete
// implementation type are exposed by the handler, not only the methods declared
// in the interface T.
func NewHandlerFor[T any](impl any) (*Handler, error) {
	if impl == nil {
		return nil, fmt.Errorf("impl must not be nil")
	}
	t := reflect.TypeFor[T]()
	n, err := serviceName(t)
	if err != nil {
		return nil, err
	}
	return newHandler(n, impl, t, false)
}

// NewStrictHandlerFor creates a new Handler for the given implementation,
// using type T as a service name and a contract that restricts which methods are exposed.
//
// Only methods that are present on T and satisfy the required signature are exposed.
//
// The implementation must fully implement RPC method subset of T:
// for every suitable method on the contract type, impl must have a method
// with the same name and a compatible signature. Otherwise, an error is returned.
//
// If T is an interface, impl must also implement T.
func NewStrictHandlerFor[T any](impl any) (*Handler, error) {
	if impl == nil {
		return nil, fmt.Errorf("impl must not be nil")
	}
	t := reflect.TypeFor[T]()
	n, err := serviceName(t)
	if err != nil {
		return nil, err
	}
	return newHandler(n, impl, t, true)
}

func serviceName(t reflect.Type) (string, error) {
	if t == nil {
		return "", fmt.Errorf("cannot infer service name from type %v", t)
	}
	var name string
	if t.Kind() == reflect.Pointer {
		name = t.Elem().Name()
	} else {
		name = t.Name()
	}
	if name == "" {
		return "", fmt.Errorf("cannot infer service name from type %v", t)
	}
	return name, nil
}

// Def is an alias for NewHandlerFor that panics if an error occurs.
func Def[T any](impl any) *Handler {
	h, err := NewHandlerFor[T](impl)
	if err != nil {
		panic(err)
	}
	return h
}

// DefService is an alias for NewHandler that panics if an error occurs.
func DefService(service string, impl any) *Handler {
	h, err := NewHandler(service, impl)
	if err != nil {
		panic(err)
	}
	return h
}

func newHandler(service string, impl any, contract reflect.Type, strict bool) (*Handler, error) {
	if impl == nil {
		return nil, fmt.Errorf("impl must not be nil")
	}

	itype := reflect.TypeOf(impl)

	if contract != nil {
		if contract.Kind() == reflect.Pointer && contract.Elem().Kind() == reflect.Interface {
			return nil, fmt.Errorf("pointers to interfaces are not supported")
		}
		if contract.Kind() == reflect.Interface {
			if !itype.Implements(contract) {
				return nil, fmt.Errorf("%v does not implement interface %v", itype, contract)
			}
		}
	}

	h := &Handler{
		name:    service,
		impl:    reflect.ValueOf(impl),
		methods: make(map[string]*methodType),
	}

	contractMethods := make(map[string]rpcSignature)
	if contract != nil {
		contractOffset := 0
		if contract.Kind() != reflect.Interface {
			contractOffset = 1
		}
		for i := 0; i < contract.NumMethod(); i++ {
			m := contract.Method(i)
			s, ok := rpcMethod(m.Type, contractOffset)
			if !ok {
				continue
			}
			contractMethods[m.Name] = s
		}
	}

	offset := 0
	if itype.Kind() != reflect.Interface {
		offset = 1
	}

	for i := 0; i < itype.NumMethod(); i++ {
		method := itype.Method(i)

		s, ok := rpcMethod(method.Type, offset)
		if !ok {
			continue
		}

		if contract != nil {
			cs, inContract := contractMethods[method.Name]
			if strict && !inContract {
				continue
			}
			if inContract {
				if s.mode != cs.mode {
					return nil, fmt.Errorf("method %v has incompatible streaming mode (expected %v)", method.Name, cs.mode)
				}
				if s.req != cs.req {
					return nil, fmt.Errorf("method %v has incompatible request type %v (expected %v)", method.Name, s.req, cs.req)
				}
				if s.res != cs.res {
					return nil, fmt.Errorf("method %v has incompatible response type %v (expected %v)", method.Name, s.res, cs.res)
				}
			}
		}

		h.methods[method.Name] = &methodType{
			fn:   h.impl.MethodByName(method.Name),
			rt:   s.req.Elem(),
			mode: s.mode,
		}
	}

	if contract != nil {
		for name := range contractMethods {
			if _, ok := h.methods[name]; !ok {
				return nil, fmt.Errorf("%v is missing required method %v", itype, name)
			}
		}
	}

	if len(h.methods) == 0 {
		return nil, fmt.Errorf("no suitable methods found on %v", itype)
	}

	return h, nil
}

type rpcSignature struct {
	req  reflect.Type
	res  reflect.Type
	mode methodMode
}

func rpcMethod(mtype reflect.Type, offset int) (rpcSignature, bool) {

	// Method(context.Context, *T1) (*T2, error)
	// Method(context.Context, *T1, io.Writer) (*T2, error)
	// Method(context.Context, *T1, io.Reader) (*T2, error)

	var sig rpcSignature

	if mtype.NumOut() != 2 {
		return sig, false
	}

	if !mtype.Out(1).AssignableTo(errorType) {
		return sig, false
	}
	sig.res = mtype.Out(0)
	if sig.res.Kind() != reflect.Pointer {
		return sig, false
	}

	/**/

	if mtype.NumIn() != 2+offset && mtype.NumIn() != 3+offset {
		return sig, false
	}
	ctxIdx := offset
	reqIdx := offset + 1

	if !mtype.In(ctxIdx).AssignableTo(contextType) {
		return sig, false
	}
	sig.req = mtype.In(reqIdx)
	if sig.req.Kind() != reflect.Pointer {
		return sig, false
	}

	if mtype.NumIn() == 2+offset {
		sig.mode = modeUnary
		return sig, true
	}

	/**/

	streamIdx := offset + 2
	arg := mtype.In(streamIdx)
	switch {
	case arg.AssignableTo(writerType):
		sig.mode = modeServerStream
		return sig, true
	case arg.AssignableTo(readerType):
		sig.mode = modeClientStream
		return sig, true
	default:
		return sig, false
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	service, method, _ := strings.Cut(strings.Trim(r.URL.Path, "/"), "/")

	if h.name != service || method == "" || strings.IndexByte(method, '/') >= 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	m, ok := h.methods[method]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	ctype := r.Header.Get("Content-Type")
	if ctype == "" {
		ctype = "application/json"
	}
	codec := codecs[ctype]
	if codec == nil {
		ctype, _, _ = mime.ParseMediaType(ctype)
		codec = codecs[ctype]
		if codec == nil {
			w.WriteHeader(http.StatusUnsupportedMediaType)
			return
		}
	}

	switch m.mode {
	case modeUnary:
		h.serveUnary(w, r, m, codec, ctype)
	case modeServerStream:
		h.serveServerStream(w, r, m, codec, ctype)
	case modeClientStream:
		h.serveClientStream(w, r, m, codec, ctype)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (h *Handler) serveUnary(w http.ResponseWriter, r *http.Request, m *methodType, codec Codec, ctype string) {
	req := reflect.New(m.rt)
	if err := codec.Decode(r.Body, req.Interface()); err != nil {
		w.Header().Set(ErrorHeader, err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if proto := r.Header.Get(ProtoHeader); proto == "N" || proto == "B" {
		args := argPool.Get().(*[]reflect.Value)
		(*args)[0] = reflect.ValueOf(context.WithoutCancel(r.Context()))
		(*args)[1] = req
		go func() {
			if _, err := m.call(args, 2); err != nil {
				log.Println("vrpc:", err)
			}
		}()
		w.WriteHeader(http.StatusOK)
		return
	}

	args := argPool.Get().(*[]reflect.Value)
	(*args)[0] = reflect.ValueOf(r.Context())
	(*args)[1] = req

	results, panicErr := m.call(args, 2)

	h.sendUnaryResponse(w, codec, ctype, results, panicErr)
}

func (h *Handler) sendUnaryResponse(w http.ResponseWriter, codec Codec, ctype string, results []reflect.Value, err error) {

	if err != nil {
		w.Header().Set(ErrorHeader, err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if errValue := results[1].Interface(); errValue != nil {
		w.Header().Set(ErrorHeader, errValue.(error).Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	res := results[0].Interface()

	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	if err = codec.Encode(buf, res); err != nil {
		w.Header().Set(ErrorHeader, err.Error())
		w.WriteHeader(StatusEncodingError)
		return
	}

	w.Header().Set("Content-Type", ctype)
	w.WriteHeader(http.StatusOK)
	_, _ = buf.WriteTo(w)
}

/**/

const (
	mEnd byte = iota
	mMsg
	mBin
	mPing
	mError
)

type framedIO struct {
	enc Encoder
	dec Decoder

	msgpackEncoder *msgpack.Encoder
	msgpackDecoder *msgpack.Decoder

	writeBuf *bufio.Writer
	flusher  http.Flusher

	readBuf []byte
	readPos int

	// framedIO is not generally thread-safe:
	// writes must be performed from a single goroutine
	//
	// but internally, it may write concurrently from a ping goroutine,
	// this internal concurrency is synchronized by this mu
	//
	// fio.mu.Lock  - ping
	// fio.mu.RLock - all others
	mu sync.RWMutex

	activity atomic.Int64 // unixnano
	flushed  atomic.Bool
	closed   atomic.Bool

	stop    chan struct{}
	stopped sync.WaitGroup
}

var fioPool = sync.Pool{
	New: func() any {
		return &framedIO{
			readBuf:  make([]byte, 0, 4<<10),
			writeBuf: bufio.NewWriterSize(io.Discard, 32<<10),
		}
	},
}

func newFramedIO(codec Codec, w io.Writer, r io.Reader) *framedIO {
	f := fioPool.Get().(*framedIO)
	*f = framedIO{
		writeBuf: f.writeBuf,
		readBuf:  f.readBuf[:0],
		readPos:  0,
	}

	if w != nil {
		f.writeBuf.Reset(w)
		enc := codec.NewEncoder(f.writeBuf)
		if mp, ok := enc.(*msgpack.Encoder); ok {
			f.msgpackEncoder = mp
		}
		f.enc = enc
		f.flusher, _ = w.(http.Flusher)
		f.activity.Store(time.Now().UnixNano())
		f.stop = make(chan struct{})
		f.startPing()
	}
	if r != nil {
		dec := codec.NewDecoder(r)
		if mp, ok := dec.(*msgpack.Decoder); ok {
			f.msgpackDecoder = mp
		}
		f.dec = dec
	}
	return f
}

func (fio *framedIO) decodeMarker() (byte, error) {
	if fio.msgpackDecoder != nil {
		return fio.msgpackDecoder.DecodeUint8()
	}
	var marker byte
	err := fio.dec.Decode(&marker)
	return marker, err
}

func (fio *framedIO) decodeString() (string, error) {
	if fio.msgpackDecoder != nil {
		return fio.msgpackDecoder.DecodeString()
	}
	var s string
	err := fio.dec.Decode(&s)
	return s, err
}

func (fio *framedIO) encodeMarker(marker byte) error {
	if fio.msgpackEncoder != nil {
		return fio.msgpackEncoder.EncodeUint8(marker)
	}
	return fio.enc.Encode(marker)
}

func (fio *framedIO) encodeBytes(b []byte) error {
	if fio.msgpackEncoder != nil {
		return fio.msgpackEncoder.EncodeBytes(b)
	}
	return fio.enc.Encode(b)
}

func (fio *framedIO) encodeString(v string) error {
	if fio.msgpackEncoder != nil {
		return fio.msgpackEncoder.EncodeString(v)
	}
	return fio.enc.Encode(v)
}

func (fio *framedIO) release() {
	fio.closed.Store(true)
	if fio.stop != nil {
		close(fio.stop)
		fio.stopped.Wait()
		fio.stop = nil
	}
	fioPool.Put(fio)
}

func (fio *framedIO) updateActivity() {
	fio.activity.Store(time.Now().UnixNano())
}

func (fio *framedIO) sendPing() error {
	fio.mu.Lock()
	defer fio.mu.Unlock()

	if fio.closed.Load() {
		return io.ErrClosedPipe
	}

	if err := fio.encodeMarker(mPing); err != nil {
		fio.closed.Store(true)
		return err
	}
	if err := fio.flush(); err != nil {
		return err
	}

	fio.updateActivity()

	return nil
}

func (fio *framedIO) sendError(err error) error {
	if err == nil {
		return nil
	}
	fio.mu.RLock()
	defer fio.mu.RUnlock()

	if fio.closed.Load() {
		return io.ErrClosedPipe
	}
	defer fio.closed.Store(true)

	if e := fio.encodeMarker(mError); e != nil {
		return e
	}
	if e := fio.encodeString(err.Error()); e != nil {
		return e
	}
	if e := fio.flush(); e != nil {
		return e
	}

	fio.updateActivity()

	return nil
}

func (fio *framedIO) sendEnd() error {
	fio.mu.RLock()
	defer fio.mu.RUnlock()

	if fio.closed.Load() {
		return io.ErrClosedPipe
	}

	defer fio.closed.Store(true)

	if err := fio.encodeMarker(mEnd); err != nil {
		return err
	}
	if err := fio.flush(); err != nil {
		return err
	}

	fio.updateActivity()

	return nil
}

func (fio *framedIO) sendMsg(v any) error {
	fio.mu.RLock()
	defer fio.mu.RUnlock()

	if fio.closed.Load() {
		return io.ErrClosedPipe
	}

	if err := fio.encodeMarker(mMsg); err != nil {
		fio.closed.Store(true)
		return err
	}
	if err := fio.enc.Encode(v); err != nil {
		fio.closed.Store(true)
		return err
	}
	if err := fio.flush(); err != nil {
		return err
	}

	fio.updateActivity()

	return nil
}

func (fio *framedIO) sendBytes(b []byte) error {

	fio.mu.RLock()
	defer fio.mu.RUnlock()

	if fio.closed.Load() {
		return io.ErrClosedPipe
	}

	if err := fio.encodeMarker(mBin); err != nil {
		fio.closed.Store(true)
		return err
	}
	if err := fio.encodeBytes(b); err != nil {
		fio.closed.Store(true)
		return err
	}

	fio.updateActivity() // without flush (leave it to the user)

	return nil
}

func (fio *framedIO) startPing() {
	fio.stopped.Add(1)
	go func() {
		defer fio.stopped.Done()
		t := time.NewTicker(1 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
			case <-fio.stop:
				return
			}
			if fio.closed.Load() {
				return
			}
			since := time.Since(time.Unix(0, fio.activity.Load()))
			if !fio.flushed.Load() && since < 13*time.Second {
				continue
			}
			if since < 4*time.Second {
				continue
			}
			if err := fio.sendPing(); err != nil {
				return
			}
		}
	}()
}

func (fio *framedIO) Flush() error {
	fio.mu.RLock()
	defer fio.mu.RUnlock()

	return fio.flush()
}

func (fio *framedIO) flush() error {
	if fio.closed.Load() {
		return io.ErrClosedPipe
	}
	if err := fio.writeBuf.Flush(); err != nil {
		return err
	}
	if fio.flusher != nil {
		fio.flusher.Flush()
	}
	fio.flushed.Store(true)

	fio.updateActivity()

	return nil
}

func (fio *framedIO) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	if err := fio.sendBytes(p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (fio *framedIO) Read(p []byte) (int, error) {
	for {
		// drain current chunk
		if fio.readPos < len(fio.readBuf) {
			n := copy(p, fio.readBuf[fio.readPos:])
			fio.readPos += n
			return n, nil
		}

		// read more
		marker, err := fio.decodeMarker()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return 0, io.ErrUnexpectedEOF
			}
			return 0, err
		}

		switch marker {
		case mPing:
			continue

		case mBin:
			fio.readBuf = fio.readBuf[:0]
			// msgpack reuses slice only if called via Decode (not DecodeBytes)
			if err = fio.dec.Decode(&fio.readBuf); err != nil {
				if errors.Is(err, io.EOF) {
					return 0, io.ErrUnexpectedEOF
				}
				return 0, err
			}
			fio.readPos = 0
			// loop will drain

		case mEnd:
			return 0, io.EOF

		case mError:
			s, err := fio.decodeString()
			if err != nil {
				if errors.Is(err, io.EOF) {
					return 0, io.ErrUnexpectedEOF
				}
				return 0, err
			}
			return 0, errors.New(s)

		case mMsg:
			return 0, &Error{"protocol error: unexpected message frame while reading binary"}

		default:
			return 0, &Error{"protocol error: unknown marker"}
		}
	}
}

func (h *Handler) serveServerStream(w http.ResponseWriter, r *http.Request, m *methodType, codec Codec, ctype string) {

	req := reflect.New(m.rt)
	if err := codec.Decode(r.Body, req.Interface()); err != nil {
		w.Header().Set(ErrorHeader, err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", ctype)

	out := newFramedIO(codec, w, nil)
	defer out.release()

	args := argPool.Get().(*[]reflect.Value)
	(*args)[0] = reflect.ValueOf(r.Context())
	(*args)[1] = req
	(*args)[2] = reflect.ValueOf(out)

	results, panicErr := m.call(args, 3)

	if panicErr != nil {
		if !out.flushed.Load() {
			w.Header().Set(ErrorHeader, panicErr.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_ = out.sendError(panicErr)
		return
	}

	if errValue := results[1].Interface(); errValue != nil {
		err := errValue.(error)
		if !out.flushed.Load() {
			w.Header().Set(ErrorHeader, err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_ = out.sendError(err)
		return
	}

	if err := out.sendMsg(results[0].Interface()); err != nil {
		_ = out.sendError(err)
		return
	}

	_ = out.sendEnd()
}

func (h *Handler) serveClientStream(w http.ResponseWriter, r *http.Request, m *methodType, codec Codec, ctype string) {

	in := newFramedIO(codec, nil, r.Body)
	defer in.release()

	marker, err := in.decodeMarker()
	if err != nil {
		w.Header().Set(ErrorHeader, err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	for marker == mPing {
		if marker, err = in.decodeMarker(); err != nil {
			w.Header().Set(ErrorHeader, err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}

	if marker != mMsg {
		w.Header().Set(ErrorHeader, "protocol error: expected message marker")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	req := reflect.New(m.rt)
	if err = in.dec.Decode(req.Interface()); err != nil {
		w.Header().Set(ErrorHeader, err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	args := argPool.Get().(*[]reflect.Value)
	(*args)[0] = reflect.ValueOf(r.Context())
	(*args)[1] = req
	(*args)[2] = reflect.ValueOf(in)

	results, panicErr := m.call(args, 3)

	h.sendUnaryResponse(w, codec, ctype, results, panicErr)
}

// Mux is a multiplexer that routes requests to specific Handlers based on the service name.
type Mux struct {
	handlers map[string]*Handler
}

// NewMux creates a new Mux with the provided list of Handlers.
// It returns an error if duplicate service names are detected.
func NewMux(handlers ...*Handler) (*Mux, error) {
	m := &Mux{
		handlers: make(map[string]*Handler, len(handlers)),
	}
	for _, h := range handlers {
		if _, exists := m.handlers[h.name]; exists {
			return nil, fmt.Errorf("duplicate service: %v", h.name)
		}
		m.handlers[h.name] = h
	}
	return m, nil
}

// Add registers a new Handler with the Mux.
// It returns an error if a service with the same name already exists.
// It is not safe to call Add concurrently after the Mux has started serving requests.
func (m *Mux) Add(handler *Handler) error {
	if _, exists := m.handlers[handler.name]; exists {
		return fmt.Errorf("duplicate service: %v", handler.name)
	}
	m.handlers[handler.name] = handler
	return nil
}

func (m *Mux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	service, method, _ := strings.Cut(strings.Trim(r.URL.Path, "/"), "/")
	if service == "" || method == "" || strings.IndexByte(method, '/') >= 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if handler, ok := m.handlers[service]; ok {
		handler.ServeHTTP(w, r)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

// Mode controls how the client routes requests.
type Mode uint8

const (
	// ModeDefault means: connect to Endpoint, send Host header equal to Endpoint host.
	ModeDefault Mode = iota

	// ServiceToHeader means: connect to Endpoint, but send Host header equal to service name.
	// Useful for sidecars/proxies that route by Host/authority.
	ServiceToHeader

	// ServiceToURL means: connect directly to http(s)://<service>/..., and also send Host header = service.
	// Endpoint must not be set in this mode.
	ServiceToURL
)

// ClientOption configures a Client in NewClient.
type ClientOption func(*clientConfig)

// WithEndpoint sets the fixed endpoint dial target (scheme://host[:port][/prefix]).
// Required for default mode and for ServiceToHeader; forbidden for ServiceToURL.
func WithEndpoint(endpoint string) ClientOption {
	return func(c *clientConfig) { c.endpoint = endpoint; c.hasEndpoint = true }
}

// WithClient sets the underlying HTTP client (transport/TLS/etc.).
// If not set, a default client is created with MaxIdleConnsPerHost set to 100.
func WithClient(hc *http.Client) ClientOption {
	return func(c *clientConfig) { c.client = hc }
}

// WithCodec sets the RPC codec. If not set, msgpack is used.
func WithCodec(codec Codec) ClientOption {
	return func(c *clientConfig) { c.codec = codec }
}

// WithMode sets routing mode. If not set, ModeDefault is used.
func WithMode(mode Mode) ClientOption {
	return func(c *clientConfig) { c.mode = mode }
}

// WithScheme sets scheme used only in ServiceToURL mode (default: http).
func WithScheme(scheme string) ClientOption {
	return func(c *clientConfig) { c.scheme = scheme; c.hasScheme = true }
}

// WithPrefix sets a path prefix (e.g. "/prefix").
// If Endpoint has a path component and WithPrefix is set, WithPrefix overrides the endpoint path.
func WithPrefix(prefix string) ClientOption {
	return func(c *clientConfig) { c.prefix = prefix; c.hasPrefix = true }
}

type clientConfig struct {
	client *http.Client
	codec  Codec
	mode   Mode

	endpoint    string
	hasEndpoint bool

	scheme    string
	hasScheme bool

	prefix    string
	hasPrefix bool
}

// Client is an RPC client.
type Client struct {
	client *http.Client
	codec  Codec
	mode   Mode
	base   url.URL
	prefix string
	ctype  []string

	paths  atomic.Pointer[map[pathKey]string]
	pathMu sync.Mutex
}

// NewClient constructs a Client using the provided options.
// It validates the resulting configuration and returns an error if it is not valid.
func NewClient(options ...ClientOption) (*Client, error) {

	cfg := &clientConfig{
		mode: ModeDefault,
	}
	for _, opt := range options {
		if opt != nil {
			opt(cfg)
		}
	}

	var base url.URL
	var prefix string

	if cfg.hasScheme && cfg.mode != ServiceToURL {
		return nil, fmt.Errorf("WithScheme is only valid with ServiceToURL mode")
	}

	switch cfg.mode {

	case ModeDefault, ServiceToHeader:

		if !cfg.hasEndpoint || strings.TrimSpace(cfg.endpoint) == "" {
			return nil, fmt.Errorf("endpoint is required for mode %v", cfg.mode)
		}
		u, err := url.Parse(cfg.endpoint)
		if err != nil {
			return nil, fmt.Errorf("invalid endpoint: %w", err)
		}
		if u.Scheme == "" {
			u.Scheme = "http"
		}
		if u.Host == "" {
			return nil, fmt.Errorf("endpoint must include host")
		}
		if cfg.hasPrefix {
			prefix = normalizePrefix(cfg.prefix)
		} else {
			prefix = normalizePrefix(u.Path)
		}
		base = url.URL{
			Scheme: u.Scheme,
			Host:   removeEmptyPort(u.Host),
			Path:   prefix,
		}

	case ServiceToURL:

		if cfg.hasEndpoint {
			return nil, fmt.Errorf("endpoint must not be set for mode ServiceToURL")
		}
		scheme := cfg.scheme
		if scheme == "" {
			scheme = "http"
		}
		if scheme != "http" && scheme != "https" {
			return nil, fmt.Errorf("unsupported scheme %q (expected http or https)", scheme)
		}
		if cfg.hasPrefix {
			prefix = normalizePrefix(cfg.prefix)
		} else {
			prefix = "/"
		}
		base = url.URL{
			Scheme: scheme,
			Host:   "unused",
			Path:   prefix,
		}

	default:
		return nil, fmt.Errorf("unknown mode %v", cfg.mode)
	}

	hc := cfg.client
	if hc == nil {
		hc = &http.Client{
			Transport: &http.Transport{
				DisableCompression:  true,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     time.Minute,
			},
		}
	}
	codec := cfg.codec
	if codec == nil {
		codec = defaultCodec
	}

	c := &Client{
		client: hc,
		codec:  codec,
		mode:   cfg.mode,
		base:   base,
		prefix: prefix,
		ctype:  []string{codec.ContentType()},
	}
	m := make(map[pathKey]string, 8)
	c.paths.Store(&m)

	return c, nil
}

// ServerStream calls an RPC method that streams data from the server to the client.
//
// The server implementation receives an io.Writer and may write the streamed payload
// to it. On the client side, all streamed bytes are forwarded into dst.
//
// Context cancellation aborts the request.
// dst may observe a write error originating from request cancellation.
//
// dst receives bytes as they arrive, but actual network delivery depends
// on server flushing. The server-side writer supports optional explicit Flush
// to prioritize latency; otherwise buffering may increase throughput.
//
// Transport and protocol errors are returned as *vrpc.Error.
// Service-level failures are returned as a regular error (not wrapped).
//
// If an error occurs, dst may have already received a prefix of the stream.
func (c *Client) ServerStream(ctx context.Context, service, method string, request any, dst io.Writer, response any) error {

	buf := new(bytes.Buffer) // do not pool here (transport may still read on http/2)

	if err := c.codec.Encode(buf, request); err != nil {
		return &Error{"error encoding request: " + err.Error()}
	}
	req := c.newRequest(ctx, service, method)
	req.Body = io.NopCloser(buf)
	req.ContentLength = int64(buf.Len())

	res, err := c.client.Do(req)
	if err != nil {
		return &Error{"request error: " + err.Error()}
	}
	defer func() { _ = res.Body.Close() }()
	defer func() { _, _ = io.Copy(io.Discard, res.Body) }()

	switch res.StatusCode {
	case http.StatusOK:

		in := newFramedIO(c.codec, nil, res.Body)
		defer in.release()

		b := make([]byte, 0, 512)
		for {
			marker, e := in.decodeMarker()
			if e != nil {
				return &Error{"error decoding stream marker: " + e.Error()}
			}
			switch marker {

			case mPing:
				continue

			case mBin:
				b = b[:0]
				if e = in.dec.Decode(&b); e != nil {
					return &Error{"error decoding binary chunk: " + e.Error()}
				}
				if _, e = dst.Write(b); e != nil {
					return e
				}

			case mMsg:
				if e = in.dec.Decode(response); e != nil {
					return &Error{"error decoding final response: " + e.Error()}
				}

			case mEnd:
				return nil

			case mError:
				s, e := in.decodeString()
				if e != nil {
					return &Error{"error decoding error message: " + e.Error()}
				}
				return errors.New(s)

			default:
				return &Error{"protocol error: unknown marker"}
			}
		}

	case http.StatusUnsupportedMediaType:
		return ErrNoCodec

	case http.StatusNotFound:
		return ErrNotFound

	case http.StatusBadRequest:
		if errText := res.Header.Get(ErrorHeader); errText != "" {
			return &Error{"server failed to decode the request: " + errText}
		}
		return &Error{"server failed to decode the request but did not provide any error"}

	case http.StatusInternalServerError:
		if errText := res.Header.Get(ErrorHeader); errText != "" {
			return errors.New(errText) // service error
		}
		return &Error{"server returned 500 without error header"}

	case StatusEncodingError:
		if errText := res.Header.Get(ErrorHeader); errText != "" {
			return &Error{"server failed to encode response: " + errText}
		}
		return &Error{"server failed to encode response but did not provide any error"}

	default:
		return &Error{"unknown error"}
	}
}

// ClientStream calls an RPC method that streams data from the client to the server.
//
// Server implementation receives an io.Reader and may read the stream until EOF,
// then returns a regular unary response which is decoded into response.
//
// End-of-stream is signaled by src returning io.EOF.
// If you want to stream data incrementally (e.g. transfer objects),
// use an io.Pipe (or a custom io.Reader) and close the writer side to signal EOF.
//
// Context cancellation aborts the request. The upload goroutine will eventually
// stop when the request is torn down; src may observe read errors.
//
// The upload path is buffered; explicit flushing is not exposed.
//
// Transport and protocol errors are returned as *vrpc.Error.
// Service-level failures are returned as a regular error (not wrapped).
// On upload errors, the returned error reflects the upload failure even if the
// server already replied (best-effort error propagation).
func (c *Client) ClientStream(ctx context.Context, service, method string, request any, src io.Reader, response any) error {
	pr, pw := io.Pipe()
	defer func() { _ = pr.Close() }()
	defer func() { _ = pw.Close() }()

	errCh := make(chan error, 1)

	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()

	go func() {
		defer close(errCh)
		defer func() { _ = pw.Close() }()

		out := newFramedIO(c.codec, pw, nil)
		defer out.release()

		done := streamCtx.Done()

		if err := out.sendMsg(request); err != nil {
			_ = pw.CloseWithError(err)
			select {
			case errCh <- err:
			case <-done:
			}
			return
		}

		if _, err := io.Copy(out, src); err != nil {
			_ = out.sendError(err) // best effort
			_ = pw.CloseWithError(err)
			select {
			case errCh <- err:
			case <-done:
			}
			return
		}

		select {
		case errCh <- out.sendEnd():
		case <-done:
		}
	}()

	req := c.newRequest(ctx, service, method)
	req.Body = pr // io.NopCloser(pr)
	req.ContentLength = -1

	res, err := c.client.Do(req)
	if err != nil {
		return &Error{"request error: " + err.Error()}
	}
	defer func() { _ = res.Body.Close() }()
	defer func() { _, _ = io.Copy(io.Discard, res.Body) }()

	switch res.StatusCode {
	case http.StatusOK:
		if err = c.codec.Decode(res.Body, response); err != nil {
			return &Error{"error decoding response: " + err.Error()}
		}
		if err = <-errCh; err != nil {
			return &Error{"error uploading request body: " + err.Error()}
		}
		return nil

	case http.StatusUnsupportedMediaType:
		return ErrNoCodec

	case http.StatusNotFound:
		return ErrNotFound

	case http.StatusBadRequest:
		if errText := res.Header.Get(ErrorHeader); errText != "" {
			return &Error{"server failed to decode the request: " + errText}
		}
		return &Error{"server failed to decode the request but did not provide any error"}

	case http.StatusInternalServerError:
		if errText := res.Header.Get(ErrorHeader); errText != "" {
			return errors.New(errText)
		}
		return &Error{"server returned 500 without error header"}

	case StatusEncodingError:
		if errText := res.Header.Get(ErrorHeader); errText != "" {
			return &Error{"server failed to encode response: " + errText}
		}
		return &Error{"server failed to encode response but did not provide any error"}

	default:
		return &Error{"unknown error"}
	}
}

// Call is a generic helper function that invokes a method on the Client
// and returns result as *T.
func Call[T any](c *Client, ctx context.Context, service, method string, req any) (*T, error) {
	res := new(T)
	if err := c.Call(ctx, service, method, req, res); err != nil {
		return nil, err
	}
	return res, nil
}

// CallFor is a generic helper function that invokes a method on the Client
// using S type as a service name and returns result as *T.
func CallFor[S any, T any](c *Client, ctx context.Context, method string, req any) (*T, error) {
	svc := reflect.TypeFor[S]().Name()
	res := new(T)
	if err := c.Call(ctx, svc, method, req, res); err != nil {
		return nil, err
	}
	return res, nil
}

// Call executes a synchronous RPC method.
// It sends the request, waits for the server to process it, and decodes the body into the response.
func (c *Client) Call(ctx context.Context, service, method string, request any, response any) error {
	return c.call(ctx, service, method, request, response, "")
}

// Notify sends the request, but does not wait for the server to process it.
// Errors are returned if encoding failed or if the server was unable to decode the request.
func (c *Client) Notify(ctx context.Context, service, method string, request any) error {
	return c.call(ctx, service, method, request, nil, "N")
}

// Beacon sends the request, but does not check the result or whether the request reached the server.
// Errors are returned only if encoding fails.
func (c *Client) Beacon(ctx context.Context, service, method string, request any) error {
	return c.call(ctx, service, method, request, nil, "B")
}

type AAA struct {
	*bytes.Buffer
}

func (c *Client) call(ctx context.Context, service, method string, request any, response any, rType string) error {

	buf := new(bytes.Buffer) // do not pool here (transport may still read on http/2)

	if err := c.codec.Encode(buf, request); err != nil {
		return &Error{"error encoding request: " + err.Error()}
	}

	req := c.newRequest(ctx, service, method)
	req.Body = io.NopCloser(buf)
	req.ContentLength = int64(buf.Len())

	if rType != "" {
		req.Header.Set(ProtoHeader, rType)
	}

	if rType == "B" {
		req = req.WithContext(context.WithoutCancel(req.Context()))
		go func(req *http.Request) {
			res, err := c.client.Do(req)
			if err != nil {
				return
			}
			defer func() { _ = res.Body.Close() }()
			defer func() { _, _ = io.Copy(io.Discard, res.Body) }()
		}(req)
		return nil
	}

	res, err := c.client.Do(req)
	if err != nil {
		return &Error{"request error: " + err.Error()}
	}
	defer func() { _ = res.Body.Close() }()
	defer func() { _, _ = io.Copy(io.Discard, res.Body) }()

	switch res.StatusCode {

	case http.StatusOK:
		if rType == "N" {
			return nil
		}
		if err = c.codec.Decode(res.Body, response); err != nil {
			return &Error{"error decoding response: " + err.Error()}
		}
		return nil

	case http.StatusUnsupportedMediaType:
		return ErrNoCodec

	case http.StatusNotFound:
		return ErrNotFound

	case http.StatusBadRequest:
		if errText := res.Header.Get(ErrorHeader); errText != "" {
			return &Error{"server failed to decode the request: " + errText}
		}
		return &Error{"server failed to decode the request but did not provide any error"}

	case http.StatusInternalServerError:
		if errText := res.Header.Get(ErrorHeader); errText != "" {
			return errors.New(errText)
		}
		return &Error{"server returned 500 without error header"}

	case StatusEncodingError:
		if errText := res.Header.Get(ErrorHeader); errText != "" {
			return &Error{"server failed to encode response: " + errText}
		}
		return &Error{"server failed to encode response but did not provide any error"}

	default:
		return &Error{"unknown error"}
	}
}

func (c *Client) newRequest(ctx context.Context, service, method string) *http.Request {
	req := defaultRequest.WithContext(ctx)

	u := c.base
	u.Path = c.getPath(service, method)

	switch c.mode {
	case ServiceToURL:
		u.Host = service
		req.Host = service
	case ServiceToHeader:
		req.Host = service
	default:
		req.Host = u.Host
	}

	req.Header = http.Header{
		"Content-Type": c.ctype,
	}
	req.URL = &u

	// buf := body.Bytes()
	// req.GetBody = func() (io.ReadCloser, error) {
	// 	r := bytes.NewReader(buf)
	// 	return io.NopCloser(r), nil
	// }

	return req
}

type pathKey struct {
	service string
	method  string
}

func (c *Client) getPath(service, method string) string {
	m := *c.paths.Load()
	if p, ok := m[pathKey{service, method}]; ok {
		return p
	}
	return c.getPathSlow(service, method)
}

func (c *Client) getPathSlow(service, method string) string {
	c.pathMu.Lock()
	defer c.pathMu.Unlock()

	key := pathKey{service, method}

	m := *c.paths.Load()
	if p, ok := (m)[key]; ok {
		return p
	}

	fullPath := path.Join(c.prefix, service, method)

	x := make(map[pathKey]string, len(m)+1)
	for k, v := range m {
		x[k] = v
	}
	x[key] = fullPath

	c.paths.Store(&x)
	return fullPath
}

func normalizePrefix(p string) string {
	p = strings.TrimSpace(p)
	if p == "" {
		return "/"
	}
	p = "/" + strings.Trim(p, "/")
	if p == "/" {
		return "/"
	}
	return p + "/"
}

var defaultRequest = &http.Request{
	Method: http.MethodPost,
}

func removeEmptyPort(host string) string {
	if strings.LastIndex(host, ":") > strings.LastIndex(host, "]") {
		return strings.TrimSuffix(host, ":")
	}
	return host
}
