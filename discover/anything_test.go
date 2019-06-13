package discover

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/lixunhuan/discover-elect/tools"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func Test_anything(t *testing.T) {
	/*===============test setting======================================*/
	try := 0
	success := 0
	const TotalClient = 99
	clients := make([]*Discover, TotalClient)
	clientHit := make([]int, TotalClient)
	/*===============test setting======================================*/

	//generate discovery host list accord with TotalClient
	clientHostListForDiscovery := GenerateClusterHostList(TotalClient)

	//time spend record
	var totalTime = int64(0)

	for i := 1; i < 100000; i++ {

		//each 20 times,recreate all client
		if i%20 == 1 {
			for i := 0; i < TotalClient; i++ {
				clients[i] = New("test-"+its(i), clientHostListForDiscovery, "localhost", ":"+its(10000+i), TotalClient/2+1, false)
			}
		}
		// set uuid for this electing
		for i := 0; i < TotalClient; i++ {
			clients[i].SetUID(uuid.New().String())
		}
		try = i
		//start all client without sequence
		for i := 0; i < TotalClient; i++ {
			RandomDelayStartClient(clients[i].InitStart)
		}

		startTime := time.Now().UnixNano()
		duration := int64(0)
		result := make(chan int, 10)
		//directly check if all client has master already ,if not ,print out bad node info
		for {
			hasNoTopNode := false
			masterIDHashTotal := uint64(0)
			for i := 0; i < TotalClient; i++ {
				if clients[i].HashTop() == false {
					hasNoTopNode = true
					break
				}
				masterIDHashTotal += uint64(tools.Hash32(clients[i].MasterId()))
			}
			if !hasNoTopNode && masterIDHashTotal/uint64(TotalClient)*uint64(TotalClient) == masterIDHashTotal {
				success++
				break
			}
			duration = (time.Now().UnixNano() - startTime) / int64(time.Millisecond)
			if duration > 30*1000 {
				break
			}
			time.Sleep(time.Millisecond * 10)
		}
		totalTime += duration
		if duration > 30*1000 {
			for i := 0; i < TotalClient; i++ {
				if !clients[i].HashTop() {
					fmt.Printf("clientID %d Ping %t Status %d master %s \n", i, clients[i].isPing, clients[i].state, clients[i].requestMaster)
				}
			}
			for i := 0; i < TotalClient; i++ {
				fmt.Printf("clientUUID : %s ,array number is %d \n", clients[i].uid, i)
				clients[i].PrintTopology()
				clients[i].Stop(&result)
			}
			fmt.Printf("GlobalPing total %d \n", GlobalPing)
			break
		}
		//fmt.Printf("|%15s|%15s|%15s|%15s|%15s|\n",_1.MasterId()[36:],_2.MasterId()[36:],_3.MasterId()[36:],_4.MasterId()[36:],_5.MasterId()[36:])
		var masterName string
		for i := 0; i < TotalClient; i++ {
			if clients[i].GetUID() == clients[0].MasterId() {
				clientHit[i]++
				masterName = "test-" + its(i)
			}
		}
		fmt.Printf("tried %d times, spend %d Millisecond, with %d pings times, master is %s \n", i, duration, GlobalPing, masterName)
		for i := 0; i < TotalClient; i++ {
			clients[i].Stop(&result)
		}
		GlobalPing = 0
		for i := 0; i < TotalClient; i++ {
			<-result
		}
		if i > 1 && i%50 == 1 {
			fmt.Printf("average spend time is %d Millisecond \n", totalTime/50)
			totalTime = 0
			//runtime.GC()
			for i := 0; i < TotalClient; i++ {
				fmt.Printf("|%d:%d|", i, clientHit[i])
				if i%20 == 0 {
					fmt.Printf("\n")
				}
			}
			//pause and easy to watch report
			time.Sleep(time.Second * 30)
		}
		println("try", try, "success", success)
		//wait all client goroutine stopped
		time.Sleep(time.Millisecond * 5000)
	}
}

func GenerateClusterHostList(TotalClient int) string {
	dsBuffer := bytes.Buffer{}
	for i := 0; i < TotalClient; i++ {
		if i != 0 {
			dsBuffer.WriteString(",")
		}
		dsBuffer.WriteString("localhost:")
		dsBuffer.WriteString(its(10000 + i))
	}
	return string(dsBuffer.Bytes())
}
func RandomDelayStartClient(c func()) {
	go func() {
		delay := time.Microsecond * time.Duration(rand.Intn(100))
		//delay=0
		time.Sleep(delay)
		c()
	}()
	//c()
}

func its(i int) string {
	return strconv.Itoa(i)
}
