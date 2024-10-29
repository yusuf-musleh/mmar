package utils

import (
	"strings"
)

func ExtractSubdomain(host string) string {
	splitDomain := strings.Split(host, ".")
	subdomains := splitDomain[:len(splitDomain)-1]
	return strings.Join(subdomains, ".")
}
