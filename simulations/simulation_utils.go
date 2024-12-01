package simulations

import "regexp"

func ExtractTunnelURL(clientStdout string) string {
	re := regexp.MustCompile(`http:\/\/[a-zA-Z0-9\-]+\.localhost:\d+`)
	return re.FindString(clientStdout)
}
