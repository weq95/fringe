package cfg

import (
	"errors"
	"net"
)

func IP() (error, string) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {

		return err, ""
	}

	for _, address := range addrs {
		if ip, ok := address.(*net.IPNet); ok && !ip.IP.IsLoopback() {
			if ip.IP.To4() != nil {
				return nil, ip.IP.String()
			}
		}
	}
	return errors.New("ip not found."), ""
}

func LocalIp(addr string) (bool, error) {
	var ip = net.ParseIP(addr)
	if ip == nil {
		return true, errors.New("Invalid IP address")
	}

	if ip.IsLoopback() || ip.IsLinkLocalMulticast() || ip.IsLinkLocalUnicast() {
		return true, nil
	}

	return false, nil
}

func GetFreePort() (int, error) {
	var addr, err = net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	var l, err01 = net.ListenTCP("tcp", addr)
	if err01 != nil {
		return 0, err01
	}

	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port, nil

}
