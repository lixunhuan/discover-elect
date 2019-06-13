package discover

import (
	"encoding/json"
	"fmt"
	"github.com/lixunhuan/discover-elect/tools"
	"github.com/lixunhuan/discover-elect/transport"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func New(uid string, discoverService string, advertiseHost string, port string, minNode int, checkMaster bool) *Discover {
	d := &Discover{
		true,
		Electing,
		true,
		discoverService,
		uid,
		advertiseHost + port,
		port,
		&NodeManagement{
			&sync.Mutex{},
			make(map[string]*Node),
		},
		nil,
		nil,
		nil, nil,
		nil,
		minNode, "", checkMaster,
	}
	return d
}
func (d *Discover) SetUID(s string) {
	d.uid = s
}
func (d *Discover) GetUID() string {
	return d.uid
}

type DState int

const (
	Electing            DState = 0
	ReElecting          DState = 1
	WaitForInvite       DState = 2
	WaitForMasterInvite DState = 3
)

type Discover struct {
	status          bool
	state           DState
	isPing          bool
	dsHost          string
	uid             string
	adHost          string
	port            string
	nodeManager     *NodeManagement
	listener        net.Listener
	infoSync        *sync.Mutex
	onMaster        func(uid string, host string)
	onNewSlave      func(uid string, host string)
	callbacks       map[string]map[*callbackFunc]func(m map[string][]byte) map[string][]byte
	minNode         int
	requestMaster   string
	needMasterCheck bool
}

func (d *Discover) InitStart() {
	d.Init()
	d.Start()
}
func (d *Discover) Init() {
	d.status = true
	d.state = Electing
	d.infoSync = &sync.Mutex{}
	d.callbacks = make(map[string]map[*callbackFunc]func(m map[string][]byte) map[string][]byte)
}
func (d *Discover) Start() {
	d.AddSelf()
	d.serve()
	go d.pings()
}
func (d *Discover) IsPing() bool {
	return d.isPing
}
func (d *Discover) RequestMaster() string {
	return d.requestMaster
}
func (d *Discover) State() DState {
	return d.state
}

func (d *Discover) IsMaster() bool {
	if d.nodeManager.Get(d.uid).top == nil {
		return false
	}
	return d.nodeManager.Get(d.uid).top.uid == d.uid
}
func (d *Discover) GetNodeList() [][3]string {
	list := make([][3]string, 1024)
	offset := 0
	d.nodeManager.Each(func(n *Node) {
		hostAndPort := strings.Split(n.adHost, ":")
		slot := [3]string{n.uid, hostAndPort[0], hostAndPort[1]}
		list[offset] = slot
		offset++
	})
	return list[0:offset]
}
func (d *Discover) MasterId() string {
	if d.nodeManager.Get(d.uid).top == nil {
		return ""
	}
	return d.nodeManager.Get(d.uid).top.uid
}

//Stop used only for test
func (d *Discover) Stop(c *chan int) {
	go func() {
		d.status = false
		d.listener.Close()
		d.infoSync.Lock()
		defer d.infoSync.Unlock()
		d.nodeManager.Clean()
		if c != nil {
			*c <- 1
		}
	}()
}
func (d *Discover) serve() {
	listener, err := net.Listen("tcp", d.port)
	if err == nil {
		d.listener = listener
		go func() {
			for {
				conn, err := listener.Accept()
				if err == nil {
					go d.done(conn)
				} else {
					return
				}

			}
		}()
	} else {
		time.Sleep(time.Millisecond * 100)
		d.serve()
		//glog.Fatal(err.Error())
	}
}
func (d *Discover) done(conn net.Conn) {
	s := transport.NewSession(conn)
	bs, err := s.ReadBytes()
	if err == nil {
		d.syncWork(bs, s)
	}
	s.Close()
	return

}
func (d *Discover) syncWork(bs []byte, s *transport.Session) {
	msg := ToMsg(bs)
	if msg.uid == d.uid && msg.msgType != RPCNode {
		return
	}
	d.infoSync.Lock()
	defer d.infoSync.Unlock()
	if d.status == false {
		return
	}
	switch msg.msgType {
	case MasterDispatch:
		if d.HashTop() && msg.uid == d.MasterId() {
			//yes my lord
			request := RecoverRequest(msg.payload)
			var m map[string][]byte
			err := json.Unmarshal(request.params, &m)
			if err == nil {
				payloadMap, err := d.HomeCall(request.funcName, m)
				payload, err := json.Marshal(payloadMap)
				if err == nil {
					resp := &Response{Success, payload}
					if err == nil {
						d.returnMasterDispatchACK(s, resp.ToBytes())
						return
					}
				}
			}
		}
		d.returnError(s)
	case AmIMaster:
		if d.HashTop() && msg.uid == d.MasterId() {
			d.returnAmIMasterACK(s)
		} else {
			d.returnError(s)
		}
	case RPCMaster:
		if d.HashTop() && d.uid == d.MasterId() {
			request := RecoverRequest(msg.payload)
			var m map[string][]byte
			json.Unmarshal(request.params, &m)
			payloadMap, err := d.HomeCall(request.funcName, m)
			if err == nil {
				payload, err := json.Marshal(payloadMap)
				if err == nil {
					resp := &Response{Success, payload}
					if err == nil {
						d.returnRPCMasterACK(s, resp.ToBytes())
						return
					}
				}
			}
		}
		resp := &Response{Failed, []byte{}}
		d.returnRPCMasterACK(s, resp.ToBytes())
	case RPCNode:
		request := RecoverRequest(msg.payload)
		var m map[string][]byte
		json.Unmarshal(request.params, &m)
		payloadMap, err := d.HomeCall(request.funcName, m)
		if err == nil {
			payload, err := json.Marshal(payloadMap)
			if err == nil {
				resp := &Response{Success, payload}
				if err == nil {
					d.returnRPCNodeACK(s, resp.ToBytes())
					return
				}
			}
		}
		resp := &Response{Failed, []byte{}}
		d.returnRPCNodeACK(s, resp.ToBytes())
	case Launch:
		if d.nodeManager.Get(d.uid).parent != nil && msg.uid == d.nodeManager.Get(d.uid).parent.uid {
			master := ToMsg(msg.payload)
			if master.msgType == MasterMsg {
				masterNode := &Node{nil, nil, time.Now(), master.uid, master.adHost}
				d.AddNode(master.uid, masterNode)
				go d.launchMaster(master.uid, master.adHost)
			}
		}
	case Report:
		reporter := d.nodeManager.Get(msg.uid)
		reportee := ToMsg(msg.payload)
		//reporter!=nil is for run test
		if reporter != nil && reporter.parent != nil && reporter.parent.uid == d.uid && reportee.msgType == ReportMsg {
			if d.AddReportee(reporter.uid, reportee.uid, reportee.adHost) {
				//if has parent
				d.onNewMember(reportee.uid, reportee.adHost)
				d.masterAsyncCast()
				//if self is master ,do launchMaster
				/*go func(){
					d.infoSync.Lock()
					defer d.infoSync.Unlock()
					master:= d.nodeManager.Get(d.uid).top
					if master!=nil && master.uid == d.uid{
						go d.launchMaster(d.uid,d.adHost)
					}
				}()*/

			}
			d.returnReportACK(s)
		} else {
			d.returnError(s)
		}
	case Ping:
		d.nodeManager.UpdateLS(msg.uid)
		if d.nodeManager.Get(d.uid) != nil && d.nodeManager.Get(d.uid).top != nil {
			d.returnReplyMasterPing(s)
		} else {
			d.returnReplyPing(s)
		}
	case JoinReq:

		if !d.childAvailable(msg.uid) {
			d.returnJoinReject(s)
			return
		}
		d.returnJoinAcknowledge(s)
		go func(toInviteUID string, toInviteHost string) {
			//maybe ,at this time,invitee has not start his server listen
			d.infoSync.Lock()
			defer d.infoSync.Unlock()
			ack := d.sendJoinInvite(toInviteHost)
			if ack == nil {
				//println(d.uid, "toInviteHost", toInviteHost, "retry")
				ack = d.sendJoinInvite(toInviteHost)
				if ack == nil {
					//println(d.uid, "toInviteHost", toInviteHost, "failed twice")
					return
				}
			}
			if ack.msgType == JoinInviteACK {
				d.AddChild(toInviteUID, toInviteHost)
				d.onNewMember(toInviteUID, toInviteHost)
				//d.masterAsyncCast()
			}
		}(msg.uid, msg.adHost)
	case JoinInvite:
		if !d.childAvailable(msg.uid) && d.state == WaitForInvite {
			fmt.Printf("%s invited me %s \n", msg.uid, d.uid)
			parent := &Node{nil, nil, time.Now(), msg.uid, msg.adHost}
			d.nodeManager.Get(d.uid).parent = parent
			d.AddNode(parent.uid, parent)
			d.nodeManager.Each(func(n *Node) {
				if n.uid != parent.uid {
					go d.report(parent.adHost, n.uid, n.adHost)
				}
			})
			d.returnJoinInviteAck(s)
			//some times ,this node has not been elected as master ,when sending invite msg,but ,now ,
			// at inviting ,this node has yet been elected ,so try check if this node is master and send asnyc noticeMaster
			if d.HashTop() && d.MasterId() == d.uid {
				d.asyncNoticeMaster(msg.adHost, d.uid, d.adHost)
			}
		} else {
			//println(msg.uid, "invite me failed", d.uid)
		}
	case JoinMasterReq:
		if d.nodeManager.Get(d.uid).top != nil && d.nodeManager.Get(d.uid).top.uid == d.uid {
			d.returnJoinMasterAcknowledge(s)
			//println("going to be master invite", msg.adHost)
			go func() {
				d.infoSync.Lock()
				defer d.infoSync.Unlock()
				childResult := d.sendJoinMasterInvite(msg.adHost)
				if childResult == nil {
					return
				}
				if childResult.msgType == JoinMasterInviteACK {
					d.AddNode(msg.uid, &Node{d.nodeManager.Get(d.uid), d.nodeManager.Get(d.uid), time.Now(), msg.uid, msg.adHost})
					d.asyncNoticeMaster(msg.adHost, d.uid, d.adHost)
					if d.onNewSlave != nil {
						go d.onNewSlave(msg.uid, msg.adHost)
					}
				}
			}()
		} else {
			d.returnError(s)
		}
	case JoinMasterInvite:
		if d.state == WaitForMasterInvite {
			parent := &Node{nil, nil, time.Now(), msg.uid, msg.adHost}
			d.nodeManager.Get(d.uid).parent = parent
			d.AddNode(parent.uid, parent)
			d.nodeManager.Each(func(n *Node) {
				if n.uid != parent.uid {
					go d.report(parent.adHost, n.uid, n.adHost)
				}
			})
			d.returnJoinMasterInviteAck(s)
		}
	}
	return
}
func (d *Discover) pings() {
	defer func() {
		d.isPing = false
	}()
	for {
		if !d.status {
			return
		}
		result := d.ping()
		if result == 0 {
			/*if d.state == WaitForInvite||d.state == WaitForMasterInvite{
				time.Sleep(time.Millisecond*500)
				self:=d.nodeManager.Get(d.uid)
				if self !=nil && self.top == nil{
					d.state = ReElecting
				}else{
					return
				}
			}*/
			return
			/*go func(){
				time.Sleep(time.Millisecond*500)
				if d.status == true{
					self:=d.nodeManager.Get(d.uid)
					if (d.state == WaitForInvite||d.state == WaitForMasterInvite)&&self !=nil && self.top == nil{
						d.state = Electing
						d.isPing = true
						go d.pings()
					}
				}

			}()*/
			//return
		}
		if d.nodeManager.Count() >= d.minNode {
			time.Sleep(time.Millisecond * 100)
			//todo register master func
			go d.launchMaster(d.uid, d.adHost)
			return
		}
		time.Sleep(time.Microsecond * 1000 * time.Duration(result))
	}
}
func (d *Discover) ping() int {
	atomic.AddUint64(&GlobalPing, 1)
	msg := d.sendPing()
	if msg == nil {
		return 5
	}
	if msg.uid == d.uid {
		return 1
	}
	switch msg.msgType {
	case ReplyPing:
		if msg.uid != d.uid && tools.Hash64(d.uid) > tools.Hash64(msg.uid) {
			//waiting for parent invite
			d.state = WaitForInvite
			msg := d.sendJoin(msg.adHost)

			if msg == nil {
				d.state = ReElecting
				return 5
			}
			d.requestMaster = msg.adHost
			if msg.msgType == JoinAcknowledge {
				return 0
			} else {
				d.state = ReElecting
			}
		}
	case ReplyMasterPing:
		//waiting for master invite
		d.state = WaitForMasterInvite
		master := d.sendJoinMasterReq(msg.adHost)
		d.requestMaster = msg.adHost

		if master != nil && master.msgType == JoinMasterAcknowledge {
			return 0
		} else {
			d.state = ReElecting
		}
	}
	return 10
}
func (d *Discover) HashTop() bool {
	return d.nodeManager.Get(d.uid) != nil && d.nodeManager.Get(d.uid).top != nil
}
func (d *Discover) PrintTopology() {
	fmt.Printf("|My name : |%10s|\n", d.uid)
	if d.nodeManager.Get(d.uid).top == nil {
		fmt.Printf("No Master\n")

	} else {
		fmt.Printf("|Master : |%10s|\n", d.nodeManager.Get(d.uid).top.uid)
	}
	d.nodeManager.Each(func(n *Node) {
		if n.parent == nil {
			fmt.Printf("|%10s| without parent\n", n.uid)
		} else {
			fmt.Printf("|%10s| has parent |%10s|\n", n.uid, n.parent.uid)
		}
	})
}
func (d *Discover) report(parentHost string, childUid string, childHost string) {
	conn, err := net.DialTimeout("tcp", parentHost, 5*time.Second)
	if err == nil {
		s := transport.NewSession(conn)
		defer s.Close()
		m := Msg{ReportMsg, childUid, childHost, []byte{}}
		payload := m.ToBytes()
		m = Msg{Report, d.uid, d.adHost, payload}
		s.WriteBytes(m.ToBytes())
		bs, err := s.ReadBytes()
		if err == nil {
			ToMsg(bs)
		}
	}
}
func (d *Discover) onNewMember(newUID string, newHost string) {
	d.masterAsyncCast()
	d.asyncReportToParent(newUID, newHost)
}
func (d *Discover) masterAsyncCast() {
	master := d.nodeManager.Get(d.uid).top
	if master != nil && master.uid == d.uid {
		go d.launchMaster(d.uid, d.adHost)
	}
}
func (d *Discover) launchMaster(masterUid string, masterHost string) {
	d.infoSync.Lock()
	defer d.infoSync.Unlock()
	masterNode := d.nodeManager.Get(masterUid)
	d.nodeManager.Each(func(n *Node) {
		n.top = masterNode
	})
	if d.onMaster != nil {
		go d.onMaster(masterUid, masterHost)
	}
	if d.needMasterCheck {
		go d.masterHeartBeat()
	}
	d.nodeManager.Each(func(n *Node) {
		if n.uid != d.uid && n.parent != nil && n.parent.uid == d.uid {
			// send to child only
			d.asyncNoticeMaster(n.adHost, masterUid, masterHost)
		}
	})
}
func (d *Discover) masterHeartBeat() {
	time.Sleep(time.Second * 10)
	if !d.HashTop() {
		panic("master dose not exist.")
	}
	if d.MasterId() != d.uid {
		for {
			time.Sleep(time.Second)
			msg := d.sendMasterPing(d.nodeManager.Get(d.MasterId()).adHost)
			if msg == nil {
				panic("master dead.")
			}
		}
	}
}
func (d *Discover) asyncNoticeMaster(targetHost string, masterUid string, masterHost string) {
	go d.noticeMaster(targetHost, masterUid, masterHost)
}
func (d *Discover) noticeMaster(targetHost string, masterUid string, masterHost string) {
	conn, err := net.DialTimeout("tcp", targetHost, 5*time.Second)
	if err == nil {
		s := transport.NewSession(conn)
		masterMsg := Msg{MasterMsg, masterUid, masterHost, []byte{}}
		m := Msg{Launch, d.uid, d.adHost, masterMsg.ToBytes()}
		s.WriteBytes(m.ToBytes())
		conn.Close()
	}
}
