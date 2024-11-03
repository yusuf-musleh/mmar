package utils

import (
	"net"
	"strings"
)

func ExtractSubdomain(host string) string {
	splitDomain := strings.Split(host, ".")
	subdomains := splitDomain[:len(splitDomain)-1]
	return strings.Join(subdomains, ".")
}

func ExtractIP(remoteAddr string) string {
	ip, _, err := net.SplitHostPort(remoteAddr)

	// Return an empty string if we could not extract IP
	if err != nil {
		return ""
	}
	return ip
}
