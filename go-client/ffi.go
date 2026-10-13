package murmur

/*
#cgo CFLAGS: -I${SRCDIR}/native
#cgo linux,amd64 LDFLAGS: -L${SRCDIR}/native/lib/linux-amd64 -lmurmur_go -ldl -lpthread -lm
#cgo linux,arm64 LDFLAGS: -L${SRCDIR}/native/lib/linux-arm64 -lmurmur_go -ldl -lpthread -lm
#cgo darwin,amd64 LDFLAGS: -L${SRCDIR}/native/lib/darwin-amd64 -lmurmur_go -framework Security -framework CoreFoundation -liconv -lresolv
#cgo darwin,arm64 LDFLAGS: -L${SRCDIR}/native/lib/darwin-arm64 -lmurmur_go -framework Security -framework CoreFoundation -liconv -lresolv
#cgo windows,amd64 LDFLAGS: -L${SRCDIR}/native/lib/windows-amd64 -lmurmur_go -lws2_32 -luserenv -lbcrypt -lntdll -lsynchronization
#include "murmur.h"
extern int32_t murmurGoCallback(uint64_t, uint32_t, uint64_t, uint8_t *, size_t, murmur_buffer *);
static int32_t go_callback(uint64_t token, uint32_t op, uint64_t scope, const uint8_t *p, size_t n, murmur_buffer *out) {
  return murmurGoCallback(token, op, scope, (uint8_t *)p, n, out);
}
static int32_t call_go(uint64_t id, const uint8_t *p, size_t n, murmur_buffer *out) {
  return murmur_call(id, p, n, go_callback, out);
}
*/
import "C"
import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"runtime/cgo"
	"sync"
	"unsafe"
)

const ABIVersion uint32 = 1

type Error struct {
	Code        string `json:"code"`
	Message     string `json:"message"`
	SQLiteCode  *int   `json:"sqlite_code"`
	NetworkCode string `json:"network_code"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && ((t.NetworkCode != "" && e.NetworkCode == t.NetworkCode) || (t.NetworkCode == "" && e.Code == t.Code))
}

var (
	ErrSQL                 = &Error{Code: "Sql"}
	ErrIO                  = &Error{Code: "Io"}
	ErrStorage             = &Error{Code: "Storage"}
	ErrWrongKey            = &Error{Code: "WrongKey"}
	ErrInvalid             = &Error{Code: "Invalid"}
	ErrCorrupt             = &Error{Code: "Corrupt"}
	ErrLimit               = &Error{Code: "Limit"}
	ErrSchemaMismatch      = &Error{Code: "SchemaMismatch"}
	ErrBusy                = &Error{Code: "Busy"}
	ErrClosed              = &Error{Code: "Closed"}
	ErrCallbackPanicked    = &Error{Code: "CallbackPanicked"}
	ErrReplicationAuth     = &Error{NetworkCode: "Auth"}
	ErrReplicationProtocol = &Error{NetworkCode: "Protocol"}
	ErrReplicationInvalid  = &Error{NetworkCode: "Invalid"}
	ErrOverloaded          = &Error{NetworkCode: "Overloaded"}
	ErrUnavailable         = &Error{NetworkCode: "Unavailable"}
)

func invalidError(message string) error { return &Error{Code: "Invalid", Message: message} }
func pointer(b []byte) *C.uint8_t {
	if len(b) == 0 {
		return nil
	}
	return (*C.uint8_t)(unsafe.Pointer(&b[0]))
}
func bufferBytes(b C.murmur_buffer) []byte {
	if b.len == 0 {
		return nil
	}
	return append([]byte(nil), unsafe.Slice((*byte)(unsafe.Pointer(b.ptr)), int(b.len))...)
}
func result(code C.int32_t, b C.murmur_buffer) ([]byte, error) {
	defer C.murmur_buffer_free(b)
	data := bufferBytes(b)
	if code == 0 {
		return data, nil
	}
	var e Error
	if err := json.Unmarshal(data, &e); err != nil {
		return nil, fmt.Errorf("invalid native error: %w", err)
	}
	return nil, &e
}
func nativeCall(id uint64, request map[string]any) ([]byte, error) {
	if uint32(C.murmur_abi_version()) != ABIVersion {
		return nil, invalidError("native ABI version mismatch")
	}
	input, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	var output C.murmur_buffer
	code := C.call_go(C.uint64_t(id), pointer(input), C.size_t(len(input)), &output)
	runtime.KeepAlive(input)
	return result(code, output)
}
func sqlCall(id uint64, op uint32, input []byte) ([]byte, error) {
	var output C.murmur_buffer
	code := C.murmur_sql(C.uint64_t(id), C.uint32_t(op), pointer(input), C.size_t(len(input)), &output)
	runtime.KeepAlive(input)
	return result(code, output)
}
func invoke(id uint64, op string, args map[string]any, out any) error {
	if args == nil {
		args = map[string]any{}
	}
	args["op"] = op
	data, err := nativeCall(id, args)
	if err != nil {
		return err
	}
	if out != nil {
		return decodeJSON(data, out)
	}
	return nil
}
func decodeJSON(data []byte, out any) error {
	// Rust serializes Duration as {secs,nanos}; Go configurations use integer ns.
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	var value any
	if err := d.Decode(&value); err != nil {
		return err
	}
	normalized, err := json.Marshal(normalize(value))
	if err != nil {
		return err
	}
	return json.Unmarshal(normalized, out)
}
func normalize(value any) any {
	switch v := value.(type) {
	case map[string]any:
		if len(v) == 2 {
			sec, s := v["secs"].(json.Number)
			ns, n := v["nanos"].(json.Number)
			if s && n {
				a, e := sec.Int64()
				b, f := ns.Int64()
				if e == nil && f == nil && a >= 0 && a <= (1<<63-1-b)/1e9 {
					return json.Number(fmt.Sprint(a*1e9 + b))
				}
			}
		}
		for k, x := range v {
			v[k] = normalize(x)
		}
	case []any:
		for i, x := range v {
			v[i] = normalize(x)
		}
	}
	return value
}

type callbackState struct {
	mu  sync.Mutex
	err error
	fn  func(uint32, uint64, []byte) ([]byte, error)
}

var callbacks sync.Map

func newCallback(fn func(uint32, uint64, []byte) ([]byte, error)) (uint64, *callbackState) {
	s := &callbackState{fn: fn}
	handle := cgo.NewHandle(s)
	id := uint64(handle)
	callbacks.Store(id, s)
	return id, s
}
func releaseCallback(id uint64) {
	if _, ok := callbacks.LoadAndDelete(id); ok {
		cgo.Handle(id).Delete()
	}
}
func (s *callbackState) failure() error { s.mu.Lock(); defer s.mu.Unlock(); return s.err }
func (s *callbackState) setError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.err = err
	}
}

//export murmurGoCallback
func murmurGoCallback(token C.uint64_t, op C.uint32_t, scope C.uint64_t, p *C.uint8_t, n C.size_t, out *C.murmur_buffer) (status C.int32_t) {
	// Rust calls from arbitrary owner/background threads. Nested SQL calls stay on this thread.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	id := uint64(token)
	if op == 0 {
		releaseCallback(id)
		return 0
	}
	value, ok := callbacks.Load(id)
	if !ok {
		return 1
	}
	s := value.(*callbackState)
	var data []byte
	defer func() {
		if panicValue := recover(); panicValue != nil {
			s.setError(&Error{Code: "CallbackPanicked", Message: fmt.Sprint(panicValue)})
			data = []byte(fmt.Sprint(panicValue))
			status = 1
		}
		*out = C.murmur_buffer_copy(pointer(data), C.size_t(len(data)))
		runtime.KeepAlive(data)
	}()
	input := append([]byte(nil), unsafe.Slice((*byte)(unsafe.Pointer(p)), int(n))...)
	var err error
	data, err = s.fn(uint32(op), uint64(scope), input)
	if err != nil {
		s.setError(err)
		data = []byte(err.Error())
		return 1
	}
	return 0
}
func callbackInvoke(id uint64, op string, args map[string]any, fn func(uint32, uint64, []byte) ([]byte, error), out any) error {
	token, state := newCallback(fn)
	defer releaseCallback(token)
	if args == nil {
		args = map[string]any{}
	}
	args["callback"] = token
	err := invoke(id, op, args, out)
	if callbackErr := state.failure(); callbackErr != nil {
		return callbackErr
	}
	return err
}

var _ = errors.Is
