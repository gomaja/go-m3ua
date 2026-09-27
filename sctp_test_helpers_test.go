package m3ua

import "github.com/gomaja/go-sctp"

func sendWithInfo(c *sctp.Conn, b []byte, info *sctp.SndInfo) (int, error) {
	return c.SendMsg(b, sctp.SendOptions{Info: info})
}

func recvWithInfo(c *sctp.Conn, b []byte) (int, *sctp.RcvInfo, error) {
	n, info, err := c.RecvMsg(b)
	return n, &info.Rcv, err
}
