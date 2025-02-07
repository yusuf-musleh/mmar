package utils

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/yusuf-musleh/mmar/constants"
)

func ExtractSubdomain(host string) string {
	splitDomain := strings.Split(host, ".")
	subdomains := splitDomain[:1]
	return strings.ToLower(strings.Join(subdomains, "."))
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

// Decode hash string to bytes so it can be compared
func decodeHash(hashStr string) ([]byte, error) {
	dst := make([]byte, hex.DecodedLen(len([]byte(hashStr))))
	n, err := hex.Decode(dst, []byte(hashStr))
	if err != nil {
		return []byte{}, err
	}
	return dst[:n], nil
}

// Check if provided Basic Auth credentials are valid
func ValidCredentials(username string, password string) bool {
	// Compute Hash for provided username and password
	usernameHash := sha256.Sum256([]byte(username))
	passwordHash := sha256.Sum256([]byte(password))

	// Receive expected Hash for username
	envUsernameHash, foundUsernameHash := os.LookupEnv("USERNAME_HASH")
	var usernameDecodedHash []byte
	if foundUsernameHash {
		var decodeErr error
		usernameDecodedHash, decodeErr = decodeHash(envUsernameHash)
		if decodeErr != nil {
			return false
		}
	} else {
		// Fallback to default if not set
		defaultUsernameHash := sha256.Sum256([]byte(constants.SERVER_STATS_DEFAULT_USERNAME))
		usernameDecodedHash = defaultUsernameHash[:]
	}

	// Receive expected Hash for password
	envPasswordHash, foundPasswordHash := os.LookupEnv("PASSWORD_HASH")
	var passwordDecodedHash []byte
	if foundPasswordHash {
		var decodeErr error
		passwordDecodedHash, decodeErr = decodeHash(envPasswordHash)
		if decodeErr != nil {
			return false
		}
	} else {
		// Fallback to default if not set
		defaultPasswordHash := sha256.Sum256([]byte(constants.SERVER_STATS_DEFAULT_PASSWORD))
		passwordDecodedHash = defaultPasswordHash[:]
	}

	// Compare them to check if they match and are valid
	validUsername := subtle.ConstantTimeCompare(usernameHash[:], usernameDecodedHash) == 1
	validPassword := subtle.ConstantTimeCompare(passwordHash[:], passwordDecodedHash) == 1
	return validUsername && validPassword
}
