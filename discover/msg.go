package discover

import (
	"bytes"
	"github.com/lixunhuan/discover-elect/tools"
)

type MsgType uint64

const (
	Error                 MsgType = 1000
	Ping                  MsgType = 0
	ReplyPing             MsgType = 1
	ReplyMasterPing       MsgType = 2
	JoinReq               MsgType = 3
	JoinAcknowledge       MsgType = 4
	JoinInvite            MsgType = 5
	JoinInviteACK         MsgType = 6
	JoinReject            MsgType = 7
	JoinMasterReq         MsgType = 8
	JoinMasterAcknowledge MsgType = 9
	JoinMasterInvite      MsgType = 10
	JoinMasterInviteACK   MsgType = 11
	Report                MsgType = 12
	ReportACK             MsgType = 13
	ReportMsg             MsgType = 14
	Launch                MsgType = 15
	MasterMsg             MsgType = 16
	RPCMaster             MsgType = 17
	RPCMasterACK          MsgType = 18
	RPCNode               MsgType = 19
	RPCNodeACK            MsgType = 20
	AmIMaster             MsgType = 21
	AmIMasterACK          MsgType = 22
	MasterDispatch        MsgType = 23
	MasterDispatchACK     MsgType = 24
)

type Msg struct {
	msgType MsgType
	uid     string
	adHost  string
	payload []byte
}

func (m *Msg) ToBytes() []byte {
	bf := bytes.Buffer{}
	bf.Write(tools.Uint64ToBytes(uint64(m.msgType)))
	bf.Write(tools.Uint64ToBytes(uint64(len(m.uid))))
	bf.Write([]byte(m.uid))
	bf.Write(tools.Uint64ToBytes(uint64(len(m.adHost))))
	bf.Write([]byte(m.adHost))
	bf.Write(tools.Uint64ToBytes(uint64(len(m.payload))))
	bf.Write(m.payload)
	return bf.Bytes()
}
func ToMsg(bs []byte) *Msg {
	start := uint64(0)
	msgType := tools.BytesToUint64(bs[start : start+8])
	start += 8
	u := tools.BytesToUint64(bs[start : start+8])
	start += 8
	uid := string(bs[start : start+u])
	start += u
	h := tools.BytesToUint64(bs[start : start+8])
	start += 8
	adHost := string(bs[start : start+h])
	start += h
	p := tools.BytesToUint64(bs[start : start+8])
	start += 8
	payload := bs[start : start+p]
	start += p
	return &Msg{
		MsgType(msgType), uid, adHost, payload,
	}
}
