package discover

import (
	"math/rand"
	"net"
	"strings"
	"time"
)

func createConn(host string) (net.Conn, error) {
	hs := strings.Split(host, ",")
	id := rand.Intn(len(hs))
	c, err := net.DialTimeout("tcp", hs[id], 5*time.Second)
	return c, err
}
