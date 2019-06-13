package transport

import (
	"bytes"
	"errors"
	"github.com/lixunhuan/discover-elect/tools"
	"net"
)

type Session struct {
	conn net.Conn
}

func NewSession(conn net.Conn) *Session {
	return &Session{conn}
}
func (s *Session) Close() error {
	return s.conn.Close()
}
func (m *MSG) WriteToSession(s *Session) error {
	return s.WriteBytes(m.ToBytes())

}

func (s *Session) ReadRequest() (RequestIf, error) {
	msg, err := s.ReadMsg()
	if err != nil {
		return nil, err
	}
	return ToRequest(msg), nil
}
func (s *Session) ReadResponse() (ResponseIf, error) {
	msg, err := s.ReadMsg()
	if err != nil {
		return nil, err
	}
	return ToResponse(msg), nil
}
func (s *Session) ReadMsg() (*MSG, error) {
	bs, err := s.ReadBytes()
	if err != nil {
		return nil, err
	}
	return fromBytes(bs)
}
func (s *Session) WriteMsg(m *MSG) error {
	return s.WriteBytes(m.ToBytes())
}
func (s *Session) WritePing() error {
	return s.WriteBytes(tools.Uint64ToBytes(uint64(Ping)))
}
func (s *Session) WriteBytes(bs []byte) error {
	s.WriteUint64(uint64(len(bs)))
	_, err := s.conn.Write(bs)
	return err
}

//func (s *Session)WriteString(str string){
//	s.WriteBytes([]byte(str))
//}
func (s *Session) WriteUint64(int uint64) {
	s.conn.Write(tools.Uint64ToBytes(int))
}

//func (s *Session)ReadString()(string,error){
//	bytes,err:=s.ReadBytes()
//	if err == nil{
//		return string(bytes),nil
//	}
//	return "",err
//}
func (s *Session) ReadUint64() (uint64, error) {
	bytes, err := s.Read(8)
	if err == nil {
		return tools.BytesToUint64(bytes), nil
	}
	return 0, err
}

func (s *Session) ReadBytes() ([]byte, error) {

	size, err := s.ReadUint64()
	if err == nil {
		return s.Read(int(size))
	}
	return nil, err
}
func (s *Session) Read(total int) ([]byte, error) {
	if total > 1024*1024*10 {
		return nil, errors.New("can not transport such big data.")
	}
	remaining := total
	var bf *bytes.Buffer = nil
	for {
		if remaining <= 0 {
			break
		}
		bs := make([]byte, remaining)
		size, err := s.conn.Read(bs)
		if err != nil {
			//if any error ,return error
			return nil, err
		}
		if size == total {
			//if and only if first time ,read all data ,directly return.
			return bs, nil
		}
		//lazy load if no all data read first time create a bytes buffer
		if bf == nil {
			bf = &bytes.Buffer{}
		}
		bf.Write(bs[0:size])
		remaining -= size
	}
	if bf == nil {
		return nil, errors.New("no data been read out.")
	}
	return bf.Bytes(), nil
}
