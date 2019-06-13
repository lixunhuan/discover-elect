package transport

import "github.com/lixunhuan/discover-elect/tools"

type ResponseIf interface {
	MSGIf
	Acknowledge() bool
}
type Response struct {
	MSG
}

func ToResponse(m *MSG) *Response {
	resp := &Response{}
	resp.Typ = m.Typ
	resp.Tenant = m.Tenant
	resp.App = m.App
	resp.Action = m.Action
	resp.Api = m.Api
	resp.Type = m.Type
	resp.Payload = m.Payload
	return resp
}
func (r *Response) Acknowledge() bool {
	return tools.BytesToUint64(r.Payload) == 0
}
