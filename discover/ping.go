package discover

import "github.com/lixunhuan/discover-elect/transport"

var GlobalPing uint64 = 0

func (d *Discover) sendPing() *Msg {
	conn, err := createConn(d.dsHost)
	if err != nil {
		return nil
	}
	defer conn.Close()
	s := transport.NewSession(conn)
	m := Msg{Ping, d.uid, d.adHost, []byte{}}
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
func (d *Discover) sendMasterPing(masterHost string) *Msg {
	conn, err := createConn(masterHost)
	if err != nil {
		return nil
	}
	defer conn.Close()
	s := transport.NewSession(conn)
	m := Msg{Ping, d.uid, d.adHost, []byte{}}
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
func (d *Discover) sendJoin(parentHost string) *Msg {
	conn, err := createConn(parentHost)
	if err != nil {
		return nil
	}
	defer conn.Close()
	s := transport.NewSession(conn)
	m := Msg{JoinReq, d.uid, d.adHost, []byte{}}
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
func (d *Discover) returnJoinInviteAck(s *transport.Session) {
	m := Msg{JoinInviteACK, d.uid, d.adHost, []byte{}}
	if err := s.WriteBytes(m.ToBytes()); err != nil {
		//println(d.uid, "write JoinInviteACK failed")
	}
}
func (d *Discover) sendJoinMasterReq(masterHost string) *Msg {
	conn, err := createConn(masterHost)
	if err != nil {
		return nil
	}
	s := transport.NewSession(conn)
	defer s.Close()
	m := Msg{JoinMasterReq, d.uid, d.adHost, []byte{}}
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
func (d *Discover) returnJoinMasterInviteAck(s *transport.Session) {
	m := Msg{JoinMasterInviteACK, d.uid, d.adHost, []byte{}}
	s.WriteBytes(m.ToBytes())
}

func (d *Discover) sendJoinInvite(childHost string) *Msg {
	conn, err := createConn(childHost)
	if err != nil {
		//println(d.adHost, "sendJoinInvite to", childHost, "create conn failed")
		return nil
	}
	defer conn.Close()
	s := transport.NewSession(conn)
	m := Msg{JoinInvite, d.uid, d.adHost, []byte{}}
	err = s.WriteBytes(m.ToBytes())
	if err != nil {
		//println("sendJoinInvite", childHost, "write error")
		return nil
	}
	bs, err := s.ReadBytes()
	if err != nil {
		//println("sendJoinInvite", childHost, "read error")
		return nil
	}
	return ToMsg(bs)
}
func (d *Discover) sendJoinMasterInvite(childHost string) *Msg {
	conn, err := createConn(childHost)
	if err != nil {
		return nil
	}
	defer conn.Close()
	s := transport.NewSession(conn)
	m := Msg{JoinMasterInvite, d.uid, d.adHost, []byte{}}
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
func (d *Discover) returnReplyMasterPing(s *transport.Session) {
	if d.nodeManager.Get(d.uid).top == nil {
		return
	}
	master := d.nodeManager.Get(d.uid).top
	m := Msg{
		ReplyMasterPing, master.uid, master.adHost, []byte{},
	}
	s.WriteBytes(m.ToBytes())
}
func (d *Discover) returnReplyPing(s *transport.Session) {
	m := Msg{
		ReplyPing, d.uid, d.adHost, []byte{},
	}
	s.WriteBytes(m.ToBytes())
}

func (d *Discover) returnRPCMasterACK(s *transport.Session, payload []byte) {
	m := Msg{
		RPCMasterACK, d.uid, d.adHost, payload,
	}
	s.WriteBytes(m.ToBytes())
}
func (d *Discover) returnRPCNodeACK(s *transport.Session, payload []byte) {
	m := Msg{
		RPCNodeACK, d.uid, d.adHost, payload,
	}
	s.WriteBytes(m.ToBytes())
}
func (d *Discover) returnJoinAcknowledge(s *transport.Session) {
	m := Msg{
		JoinAcknowledge, d.uid, d.adHost, []byte{},
	}
	s.WriteBytes(m.ToBytes())
}

func (d *Discover) returnJoinReject(s *transport.Session) {
	m := Msg{
		JoinReject, d.uid, d.adHost, []byte{},
	}
	s.WriteBytes(m.ToBytes())
}
func (d *Discover) returnJoinMasterAcknowledge(s *transport.Session) {
	m := Msg{
		JoinMasterAcknowledge, d.uid, d.adHost, []byte{},
	}
	s.WriteBytes(m.ToBytes())
}
func (d *Discover) returnReportACK(s *transport.Session) {
	m := Msg{
		ReportACK, d.uid, d.adHost, []byte{},
	}
	s.WriteBytes(m.ToBytes())
}
func (d *Discover) returnAmIMasterACK(s *transport.Session) {
	m := Msg{
		AmIMasterACK, d.uid, d.adHost, []byte{},
	}
	s.WriteBytes(m.ToBytes())
}
func (d *Discover) returnMasterDispatchACK(s *transport.Session, payload []byte) {
	m := Msg{
		MasterDispatchACK, d.uid, d.adHost, payload,
	}
	s.WriteBytes(m.ToBytes())
}

func (d *Discover) returnError(s *transport.Session) {
	m := Msg{
		Error, d.uid, d.adHost, []byte{},
	}
	s.WriteBytes(m.ToBytes())
}
func (d *Discover) asyncReportToParent(uid string, host string) {
	if uid == d.uid {
		return
	}
	self := d.nodeManager.Get(d.uid)
	if self.parent != nil {
		go func() {
			//todo report new child to parent
			d.report(self.parent.adHost, uid, host)
		}()
	}
}
