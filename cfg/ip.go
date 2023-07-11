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
