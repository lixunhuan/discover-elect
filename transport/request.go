package transport

import (
	"github.com/lixunhuan/discover-elect/tools"
)

type RequestIf interface {
	MSGIf
	WriteDataCount() uint64
	WriteDataSize() uint64
	ReadDataCount() uint64
	ReadDataSize() uint64
	GetSettingCount() uint64
	GetSettingSize() uint64
	WriteSettingCount() uint64
	WriteSettingSize() uint64
	CatClusterCount() uint64
	CatClusterSize() uint64
	GetClusterCount() uint64
	GetClusterSize() uint64
	IsDoneRecord() bool
	Count() uint64
	Size() uint64
}
type Request struct {
	MSG
	writeDataCount    uint64
	writeDataSize     uint64
	readDataCount     uint64
	readDataSize      uint64
	getSettingCount   uint64
	getSettingSize    uint64
	writeSettingCount uint64
	writeSettingSize  uint64
	catClusterCount   uint64
	catClusterSize    uint64
	getClusterCount   uint64
	getClusterSize    uint64
}

func (r *Request) WriteDataCount() uint64 {
	return r.writeDataCount
}

func (r *Request) WriteDataSize() uint64 {
	return r.writeDataSize
}

func (r *Request) ReadDataCount() uint64 {
	return r.readDataCount
}

func (r *Request) ReadDataSize() uint64 {
	return r.readDataSize
}

func (r *Request) GetSettingCount() uint64 {
	return r.getSettingCount
}

func (r *Request) GetSettingSize() uint64 {
	return r.getSettingSize
}

func (r *Request) WriteSettingCount() uint64 {
	return r.writeSettingCount
}

func (r *Request) WriteSettingSize() uint64 {
	return r.writeSettingSize
}

func (r *Request) CatClusterCount() uint64 {
	return r.catClusterCount
}

func (r *Request) CatClusterSize() uint64 {
	return r.catClusterSize
}

func (r *Request) GetClusterCount() uint64 {
	return r.getClusterCount
}

func (r *Request) GetClusterSize() uint64 {
	return r.getClusterSize
}

func (r *Request) IsDoneRecord() bool {
	return r.catClusterCount+r.getClusterCount+r.getSettingCount+r.readDataCount == 0
}

func (r *Request) Count() uint64 {
	switch r.Type {
	case WriteData:
		return r.writeDataCount
	case ReadData:
		return r.readDataCount
	case GetSetting:
		return r.GetSettingCount()
	case WriteSetting:
		return r.writeSettingCount
	case CatCLuster:
		return r.catClusterCount
	case GetCluster:
		return r.getClusterCount
	}
	return 0
}

func (r *Request) Size() uint64 {
	switch r.Type {
	case WriteData:
		return r.writeDataSize
	case ReadData:
		return r.readDataSize
	case GetSetting:
		return r.GetSettingSize()
	case WriteSetting:
		return r.writeSettingSize
	case CatCLuster:
		return r.catClusterSize
	case GetCluster:
		return r.getClusterSize
	}
	return 0
}

func ToRequest(m *MSG) *Request {
	req := &Request{}
	req.Typ = m.Typ
	req.Tenant = m.Tenant
	req.App = m.App
	req.Action = m.Action
	req.Api = m.Api
	req.Type = m.Type
	req.Payload = m.Payload
	switch req.Type {
	case WriteData:
		req.writeDataCount = tools.BytesToUint64(m.Payload[0:8])
		req.writeDataSize = tools.BytesToUint64(m.Payload[8:16])
	case ReadData:
		req.readDataCount = tools.BytesToUint64(m.Payload[0:8])
		req.readDataSize = tools.BytesToUint64(m.Payload[8:16])
	case GetSetting:
		req.getSettingCount = tools.BytesToUint64(m.Payload[0:8])
		req.getSettingSize = tools.BytesToUint64(m.Payload[8:16])
	case WriteSetting:
		req.writeSettingCount = tools.BytesToUint64(m.Payload[0:8])
		req.writeSettingSize = tools.BytesToUint64(m.Payload[8:16])
	case CatCLuster:
		req.catClusterCount = tools.BytesToUint64(m.Payload[0:8])
		req.catClusterSize = tools.BytesToUint64(m.Payload[8:16])
	case GetCluster:
		req.getClusterCount = tools.BytesToUint64(m.Payload[0:8])
		req.getClusterSize = tools.BytesToUint64(m.Payload[8:16])
	}
	return req
}
