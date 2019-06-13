package discover

import (
	"encoding/json"
	"errors"
	"github.com/lixunhuan/discover-elect/transport"
	"time"
)

func (d *Discover) RegisterOnMasterCallback(c func(uid string, host string)) {
	d.onMaster = c
}
func (d *Discover) RegisterOnNewSlaveCallback(c func(uid string, host string)) {
	d.onNewSlave = c
}

type callbackFunc func(m map[string][]byte) map[string][]byte

func toFuncType(f func(m map[string][]byte) map[string][]byte) *callbackFunc {
	return (*callbackFunc)(&f)
}
func (d *Discover) Register(name string, c func(m map[string][]byte) map[string][]byte) {
	d.infoSync.Lock()
	defer d.infoSync.Unlock()
	if d.callbacks[name] == nil {
		d.callbacks[name] = make(map[*callbackFunc]func(m map[string][]byte) map[string][]byte)
	}
	p := toFuncType(c)
	if d.callbacks[name][p] == nil {
		d.callbacks[name][p] = c
	}
}
func (d *Discover) RemoveAllRegisteredCallback(name string) {
	d.infoSync.Lock()
	defer d.infoSync.Unlock()
	if d.callbacks[name] != nil {
		d.callbacks[name] = nil
	}
}
func (d *Discover) Unregister(name string, c func(m map[string][]byte) map[string][]byte) {
	d.infoSync.Lock()
	defer d.infoSync.Unlock()
	if d.callbacks[name] == nil {
		return
	}
	p := toFuncType(c)
	if d.callbacks[name][p] != nil {
		d.callbacks[name][p] = nil
	}
}
func (d *Discover) HomeCall(funcName string, params map[string][]byte) (map[string][]byte, error) {
	//home call will be called by listener ,witch has the infoSync lock,
	if d.callbacks[funcName] == nil || len(d.callbacks[funcName]) == 0 {
		return nil, errors.New("function not found.")
	}
	for _, function := range d.callbacks[funcName] {
		return function(params), nil
	}
	return nil, errors.New("function not found.")
}
func (d *Discover) RemoteCall(funcName string, params map[string][]byte, nodeHost string) (map[string][]byte, error) {

	b, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	request := &Request{funcName, b}
	result := d.sendToNode(request.ToBytes(), nodeHost)
	if result != nil && result.msgType == RPCNodeACK {
		response := RecoverResponse(result.payload)
		if response.code == Success {
			var m map[string][]byte
			if json.Unmarshal(response.payload, &m) == nil {
				return m, nil
			}
		}
	}

	return nil, errors.New("error result")

}
func (d *Discover) MasterCall(funcName string, params map[string][]byte) error {
	if d.MasterId() == d.uid {
		b, err := json.Marshal(params)
		if err != nil {
			return err
		}
		d.infoSync.Lock()
		defer d.infoSync.Unlock()

		request := &Request{funcName, b}
		d.nodeManager.Each(func(n *Node) {
			if n.uid != d.uid {
				for {
					ask := d.pingSlave(n.adHost)
					if ask != nil && ask.msgType == AmIMasterACK {
						result := d.dispatchToSlave(n.adHost, request.ToBytes())
						if result != nil && result.msgType == MasterDispatchACK {
							response := RecoverResponse(result.payload)
							if response.code == Success {
								return
							}
						}
					}
					time.Sleep(time.Second)
				}
			}
		})
	}
	return nil

}
func (d *Discover) MasterDirectlyCall(uid string, funcName string, params map[string][]byte) error {
	if d.MasterId() == d.uid {
		b, err := json.Marshal(params)
		if err != nil {
			return err
		}
		d.infoSync.Lock()
		defer d.infoSync.Unlock()

		request := &Request{funcName, b}
		d.nodeManager.Each(func(n *Node) {
			if n.uid != d.uid && n.uid == uid {
				for {
					ask := d.pingSlave(n.adHost)
					if ask != nil && ask.msgType == AmIMasterACK {
						result := d.dispatchToSlave(n.adHost, request.ToBytes())
						if result != nil && result.msgType == MasterDispatchACK {
							response := RecoverResponse(result.payload)
							if response.code == Success {
								return
							}
						}
					}
					time.Sleep(time.Second)
				}
			}
		})
	}
	return nil

}
func (d *Discover) sendToMaster(payload []byte) *Msg {
	self := d.nodeManager.Get(d.uid)
	if self.top == nil {
		return nil
	}
	conn, err := createConn(self.top.adHost)
	if err != nil {
		return nil
	}
	defer conn.Close()
	s := transport.NewSession(conn)
	m := Msg{RPCMaster, d.uid, d.adHost, payload}
	err = s.WriteBytes(m.ToBytes())
	if err != nil {
		return nil
	}
	bs, err := s.ReadBytes()
	if err != nil {
		return nil
	}
	return ToMsg(bs)
}
func (d *Discover) sendToNode(payload []byte, slaveHost string) *Msg {
	conn, err := createConn(slaveHost)
	if err != nil {
		return nil
	}
	defer conn.Close()
	s := transport.NewSession(conn)
	m := Msg{RPCNode, d.uid, d.adHost, payload}
	err = s.WriteBytes(m.ToBytes())
	if err != nil {
		return nil
	}
	bs, err := s.ReadBytes()
	if err != nil {
		return nil
	}
	return ToMsg(bs)
}
func (d *Discover) dispatchToSlave(host string, payload []byte) *Msg {
	conn, err := createConn(host)
	if err != nil {
		return nil
	}
	defer conn.Close()
	s := transport.NewSession(conn)
	m := Msg{MasterDispatch, d.uid, d.adHost, payload}
	err = s.WriteBytes(m.ToBytes())
	if err != nil {
		return nil
	}
	bs, err := s.ReadBytes()
	if err != nil {
		return nil
	}
	return ToMsg(bs)
}
func (d *Discover) pingSlave(host string) *Msg {
	conn, err := createConn(host)
	if err != nil {
		return nil
	}
	defer conn.Close()
	s := transport.NewSession(conn)
	m := Msg{AmIMaster, d.uid, d.adHost, []byte{}}
	err = s.WriteBytes(m.ToBytes())
	if err != nil {
		return nil
	}
	bs, err := s.ReadBytes()
	if err != nil {
		return nil
	}
	return ToMsg(bs)
}
