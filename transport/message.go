package transport

import (
	"bytes"
	"errors"
	"github.com/lixunhuan/discover-elect/tools"
)

type MsgType uint64

const (
	Ping         MsgType = 0x00000000
	Msg          MsgType = 0x00000001
	All          MsgType = 0xffff0000
	WriteData    MsgType = 0xffff0001
	ReadData     MsgType = 0xffff0002
	GetSetting   MsgType = 0xffff0003
	WriteSetting MsgType = 0xffff0004
	CatCLuster   MsgType = 0xffff0005
	GetCluster   MsgType = 0xffff0006
)

type MSGIf interface {
	GetTyp() MsgType
	GetTenant() string
	GetApp() string
	GetAction() string
	GetApi() string
	GetType() MsgType
	GetPayload() []byte
	ToBytes() []byte
}
type MSG struct {
	Typ     MsgType
	Tenant  string
	App     string
	Action  string
	Api     string
	Type    MsgType
	Payload []byte
}

func (m *MSG) GetTyp() MsgType {
	return m.Typ
}
func (m *MSG) GetTenant() string {
	return m.Tenant
}

func (m *MSG) GetApp() string {
	return m.App
}

func (m *MSG) GetAction() string {
	return m.Action
}

func (m *MSG) GetApi() string {
	return m.Api
}

func (m *MSG) GetType() MsgType {
	return m.Type
}

func (m *MSG) GetPayload() []byte {
	return m.Payload
}

func (m *MSG) ToBytes() []byte {
	if m.Typ == Ping {
		return tools.Uint64ToBytes(uint64(Ping))
	}
	bf := &bytes.Buffer{}
	bf.Write(tools.Uint64ToBytes(uint64(Msg)))
	bf.Write(tools.Uint64ToBytes(uint64(len(m.Tenant))))
	bf.Write([]byte(m.Tenant))
	bf.Write(tools.Uint64ToBytes(uint64(len(m.App))))
	bf.Write([]byte(m.App))
	bf.Write(tools.Uint64ToBytes(uint64(len(m.Action))))
	bf.Write([]byte(m.Action))
	bf.Write(tools.Uint64ToBytes(uint64(len(m.Api))))
	bf.Write([]byte(m.Api))
	bf.Write(tools.Uint64ToBytes(uint64(m.Type)))
	bf.Write(tools.Uint64ToBytes(uint64(len(m.Payload))))
	bf.Write(m.Payload)
	return bf.Bytes()
}

const (
	Uint64Length = uint64(8)
)

func fromBytes(bs []byte) (*MSG, error) {

	offset := uint64(0)
	typ, offset, err := bytesUint64Reader(bs, offset)
	if MsgType(typ) == Ping {
		m := &MSG{}
		m.Typ = Ping
		return m, nil
	}
	tenant, offset, err := bytesStringReader(bs, offset)
	if err != nil {
		return nil, err
	}
	app, offset, err := bytesStringReader(bs, offset)
	if err != nil {
		return nil, err
	}
	action, offset, err := bytesStringReader(bs, offset)
	if err != nil {
		return nil, err
	}
	api, offset, err := bytesStringReader(bs, offset)
	if err != nil {
		return nil, err
	}
	tp, offset, err := bytesUint64Reader(bs, offset)
	if err != nil {
		return nil, err
	}
	payload, offset, err := bytesBytesReader(bs, offset)
	if err != nil {
		return nil, err
	}
	return &MSG{
		MsgType(typ),
		tenant,
		app,
		action,
		api,
		MsgType(tp),
		payload,
	}, nil
}
func bytesBytesReader(bs []byte, offset uint64) ([]byte, uint64, error) {
	total := uint64(len(bs))
	if offset+Uint64Length > total {
		return nil, 0, errors.New("byte array read over size")
	}
	bytesLength := tools.BytesToUint64(bs[offset : offset+Uint64Length])
	offset += Uint64Length

	if offset+bytesLength > total {
		return nil, 0, errors.New("byte array read over size")
	}
	return bs[offset : offset+bytesLength], offset + bytesLength, nil
}
func bytesStringReader(bs []byte, offset uint64) (string, uint64, error) {
	total := uint64(len(bs))
	if offset+Uint64Length > total {
		return "", 0, errors.New("byte array read over size")
	}
	stringLength := tools.BytesToUint64(bs[offset : offset+Uint64Length])
	offset += Uint64Length
	if offset+stringLength > total {
		return "", 0, errors.New("byte array read over size")
	}
	return string(bs[offset : offset+stringLength]), offset + stringLength, nil
}
func bytesUint64Reader(bs []byte, offset uint64) (uint64, uint64, error) {
	total := uint64(len(bs))
	if offset+Uint64Length > total {
		return 0, 0, errors.New("byte array read over size")
	}
	return tools.BytesToUint64(bs[offset : offset+Uint64Length]), offset + Uint64Length, nil
}
