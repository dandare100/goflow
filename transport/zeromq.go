package transport

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"net"

	flowmessage "github.com/cloudflare/goflow/v3/pb"
	"github.com/cloudflare/goflow/v3/utils"
	zmq "github.com/pebbe/zmq4"
)

var (
	//ZMQBindAddress is the address on the host to which the publisher will bind
	ZMQBindAddress *string
	//ZMQBindPort is the port on the host to which the publisher will bind
	ZMQBindPort *int
	//ZMQSourceID identifies the collector
	ZMQSourceID *int
)

//This struct is the header for the envelope ZMQ message
type zmqMsgHdr struct {
	url               [16]byte
	version, sourceID uint8
	size              uint16
	msgID             uint32
}

//ZMQState is a struct used to wrap references passed back to the caller
type ZMQState struct {
	//pointer type
	socket *zmq.Socket
}

//RegisterZMQFlags is called by the main init() and allows the registration of the ZMQ flags
func RegisterZMQFlags() {
	ZMQBindAddress = flag.String("zmq.bindaddress", "127.0.0.1", "The ZeroMQ publisher bind address")
	ZMQBindPort = flag.Int("zmq.bindport", 5556, "The ZeroMQ publisher bind port")
	ZMQSourceID = flag.Int("zmq.sourceid", 162, "The ZeroMQ source id")
}

//StartZMQPublisher starts the publisher on the specified ip and port and returns a struct with a pointer to the publisher
func StartZMQPublisher(log utils.Logger) (*ZMQState, error) {

	url := fmt.Sprintf("tcp://%s:%d", *ZMQBindAddress, *ZMQBindPort)
	log.Infof("Starting zmq publisher on : %s", url)

	publisher, err := zmq.NewSocket(zmq.PUB)
	if err != nil {
		return nil, err
	}

	err = publisher.Bind(url)
	if err != nil {
		return nil, err
	}

	zmqState := ZMQState{socket: publisher}

	return &zmqState, nil

}

//package level msgID
var cnt uint32 = 1

//PublishZMQFlowMessage accepts a flow message and publishes it to the ZMQ "flow" topic
func (s ZMQState) PublishZMQFlowMessage(msg *flowmessage.FlowMessage) {

	var binBuf bytes.Buffer

	//create the JSON string
	jsonMessage := FlowMessageToNFJSON(msg)

	//create the envelope header
	// 102, 108, 111, 119 is "flow"
	//version is 2 :-)
	//sourceID identifies the collector
	h := zmqMsgHdr{url: [16]byte{102, 108, 111, 119}, version: 2, sourceID: uint8(*ZMQSourceID), size: uint16(len(jsonMessage)), msgID: cnt}
	binary.Write(&binBuf, binary.BigEndian, h)

	s.socket.SendBytes(binBuf.Bytes(), zmq.SNDMORE)
	s.socket.Send(jsonMessage, 0)

	cnt++
}

//Publish loops through the passed slaice and publishes each flow message in turn
func (s ZMQState) Publish(msgs []*flowmessage.FlowMessage) {
	for _, message := range msgs {
		s.PublishZMQFlowMessage(message)
	}
}

//FlowMessageToNFJSON converts the protobuf info into the required json
func FlowMessageToNFJSON(fmsg *flowmessage.FlowMessage) string {
	srcmac := make([]byte, 8)
	dstmac := make([]byte, 8)
	binary.BigEndian.PutUint64(srcmac, fmsg.SrcMac)
	binary.BigEndian.PutUint64(dstmac, fmsg.DstMac)
	srcmac = srcmac[2:8]
	dstmac = dstmac[2:8]

	s := fmt.Sprintf("{\"56\":\"%v\",\"57\":\"%v\",\"10\":%v,\"14\":%v,\"58\":%v,\"8\":\"%v\",\"12\":\"%v\",\"7\":%v,\"11\":%v,\"27\":\"::\",\"28\":\"::\""+
		",\"60\":%v,\"4\":%v,\"1\":%v,\"2\":%v,\"23\":0,\"24\":0,\"22\":%v,\"21\":%v,\"130\":\"%v\",\"34\":%v,\"42\":%v,\"225\":\"%v\",\"226\":\"%v\",\"227\":%v,\"228\":%v}",
		net.HardwareAddr(srcmac), net.HardwareAddr(dstmac), fmsg.InIf, fmsg.OutIf, fmsg.SrcVlan, net.IP(fmsg.SrcAddr),
		net.IP(fmsg.DstAddr), fmsg.SrcPort, fmsg.DstPort, 4, fmsg.Proto, fmsg.Bytes, fmsg.Packets, fmsg.TimeFlowStart,
		fmsg.TimeFlowEnd, net.IP(fmsg.SamplerAddress), fmsg.SamplingRate, fmsg.SequenceNum, net.IP(fmsg.PostNatSrcIpV4Addr),
		net.IP(fmsg.PostNatDstIpV4Addr), fmsg.PostNaptSrcTransportPort, fmsg.PostNaptDstTransportPort,
	)

	return s
}
