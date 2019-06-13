package discover

import (
	"bytes"
	"github.com/lixunhuan/discover-elect/tools"
)

type Request struct {
	funcName string
	params   []byte // marshall map to byte
}

func (r *Request) ToBytes() []byte {
	bs := bytes.Buffer{}
	bs.Write(tools.Uint64ToBytes(uint64(len(r.funcName))))
	bs.WriteString(r.funcName)
	bs.Write(tools.Uint64ToBytes(uint64(len(r.params))))
	bs.Write(r.params)
	return bs.Bytes()
}
func RecoverRequest(b []byte) *Request {
	offset := uint64(0)
	nameLen := tools.BytesToUint64(b[offset : offset+8])
	offset += 8
	funcName := string(b[offset : offset+nameLen])
	offset += nameLen
	paramsLen := tools.BytesToUint64(b[offset : offset+8])
	offset += 8
	params := b[offset : offset+paramsLen]
	offset += paramsLen
	return &Request{funcName, params}
}

type ResultCode int

const (
	Success ResultCode = 0
	Failed  ResultCode = 1
)

type Response struct {
	code    ResultCode
	payload []byte // marshall map to byte
}

func (r *Response) ToBytes() []byte {
	bs := bytes.Buffer{}
	bs.Write(tools.Uint64ToBytes(uint64(r.code)))
	bs.Write(tools.Uint64ToBytes(uint64(len(r.payload))))
	bs.Write(r.payload)
	return bs.Bytes()
}
func RecoverResponse(b []byte) *Response {
	code := ResultCode(tools.BytesToUint64(b[0:8]))
	if code != Success {
		return &Response{code, nil}
	}
	len := tools.BytesToUint64(b[8:16])
	return &Response{code, b[16 : len+16]}
}
