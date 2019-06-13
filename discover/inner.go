package discover

import (
	"github.com/lixunhuan/discover-elect/tools"
	"time"
)

func (d *Discover) childAvailable(uid string) bool {
	return tools.Hash64(uid) > tools.Hash64(d.uid)
}
func (d *Discover) AddSelf() {
	d.nodeManager.Add(d.uid, &Node{nil, nil, time.Now(), d.uid, d.adHost})
}
func (d *Discover) AddNode(key string, node *Node) {
	if key == d.uid {
		return
	}
	d.nodeManager.Add(key, node)
}
func (d *Discover) AddChild(uid string, host string) {
	if uid == d.uid {
		return
	}
	d.nodeManager.Add(uid, &Node{nil, d.nodeManager.Get(d.uid), time.Now(), uid, host})
}
func (d *Discover) AddReportee(reporterUid string, uid string, host string) bool {
	if uid == d.uid && reporterUid == uid && d.nodeManager.Get(reporterUid) == nil {
		return false
	}
	if reporterUid == uid {
		return false
	}
	d.nodeManager.Add(uid, &Node{nil, d.nodeManager.Get(reporterUid), time.Now(), uid, host})
	return true
}
