package discover

import (
	"sync"
	"time"
)

type Node struct {
	top    *Node
	parent *Node
	ls     time.Time
	uid    string
	adHost string
}
type NodeManagement struct {
	lock    *sync.Mutex
	mapping map[string]*Node
}

func (nm *NodeManagement) Each(c func(n *Node)) {
	nm.lock.Lock()
	l := make([]*Node, len(nm.mapping))
	i := 0
	for _, v := range nm.mapping {
		l[i] = v
		i++
	}
	defer nm.lock.Unlock()
	for i := 0; i < len(l); i++ {
		c(l[i])
	}
}
func (nm *NodeManagement) Add(uid string, n *Node) {
	nm.lock.Lock()
	defer nm.lock.Unlock()
	nm.mapping[uid] = n
}
func (nm *NodeManagement) Clean() {
	nm.lock.Lock()
	defer nm.lock.Unlock()
	nm.mapping = make(map[string]*Node)
}
func (nm *NodeManagement) Get(uid string) *Node {
	nm.lock.Lock()
	defer nm.lock.Unlock()
	return nm.mapping[uid]
}
func (nm *NodeManagement) UpdateLS(uid string) {
	nm.lock.Lock()
	defer nm.lock.Unlock()
	if nm.mapping[uid] != nil {
		nm.mapping[uid].ls = time.Now()
	}
}
func (nm *NodeManagement) Count() int {
	nm.lock.Lock()
	defer nm.lock.Unlock()
	return len(nm.mapping)
}
func (nm *NodeManagement) OnMaster() {
	//if self is master,try check ls and report
}
