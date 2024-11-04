package utils

import (
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/yusuf-musleh/mmar/constants"
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

func MmarVersionUsage() {
	fmt.Fprintf(os.Stdout, "Prints the installed version of mmar.")
}

func MmarUsage() {
	intro := `mmar is an HTTP tunnel that exposes your localhost to the world on a public URL.

Usage:
  mmar <command> [command flags]`
	fmt.Fprintln(os.Stdout, intro)

	fmt.Fprint(os.Stdout, "\nCommands:\n")

	commands := ""
	for _, subcommand := range constants.MMAR_SUBCOMMANDS {
		command := strings.Join(subcommand, "\n    ")
		commands = commands + "  " + command + "\n"
	}

	fmt.Fprintln(os.Stdout, commands)

	fmt.Fprintln(os.Stdout)
	fmt.Fprintf(os.Stdout, "Run `mmar <command> -h` to get help for a specific command\n\n")
}
