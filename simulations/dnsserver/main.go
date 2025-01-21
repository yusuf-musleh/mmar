package dnsserver

import (
	"encoding/binary"
	"encoding/hex"
	"log"
	"net"
)

const (
	LOCALHOST_DNS_SERVER = "127.0.0.1:3535"
)

// The purpose of this DNS server is resolve requests to localhost
// addresses with subdomains. By default Go does not resolve localhost
// addresses containing subdomains, so this basic DNS server always resolves
// to the IPv6 loopback address "::1"
func StartDnsServer() {
	addr, err := net.ResolveUDPAddr("udp", LOCALHOST_DNS_SERVER)
	if err != nil {
		log.Fatal("Failed to resolve UDP Address", err)
	}

	udpConn, err := net.ListenUDP("udp", addr)

	if err != nil {
		log.Fatal("Failed to start Dummy DNS server", err)
	}

	for {
		buffer := make([]byte, 512)
		n, udpWriteAddr, err := udpConn.ReadFromUDP(buffer)
		if err != nil {
			log.Fatal("Failed to read from UDP connection", err)
		}

		go handleDnsConn(udpConn, buffer, n, udpWriteAddr)
	}
}

// Handles building and returning the response for the DNS request, that resolves to ::1
// For more details on the message format: https://datatracker.ietf.org/doc/html/rfc1035#autoid-39
func handleDnsConn(udpConn *net.UDPConn, buffer []byte, n int, udpWriteAddr *net.UDPAddr) {
	// Extracting information from DNS request
	transactionID := buffer[:2]
	questionsCount := buffer[4:6]
	authorityRRs := buffer[8:10]
	msgQuestion := buffer[12:n]

	// Building DNS response
	respBuffer := []byte{}
	respBuffer = append(respBuffer, transactionID...)

	// Adding Response flag
	respFlag, _ := hex.DecodeString("8000") // Bits: 1000 0000 0000 0000
	respBuffer = append(respBuffer, respFlag...)

	// Adding QuestionsCount
	respBuffer = append(respBuffer, questionsCount...)

	// Adding Answers
	answer, _ := hex.DecodeString("0001")
	respBuffer = append(respBuffer, answer...)

	// Adding Authorities
	respBuffer = append(respBuffer, authorityRRs...)

	// Adding Additionals (there are none)
	respBuffer = append(respBuffer, byte(0))
	respBuffer = append(respBuffer, byte(0))

	// Adding the Name (eg: ikyx31.localhost)
	i := 0
	for i < n && hex.EncodeToString(msgQuestion[i:i+1]) != "00" {
		label := int(msgQuestion[i])
		for labelI := i; labelI < (i + label + 1); labelI++ {
			respBuffer = append(respBuffer, msgQuestion[labelI])
		}
		i = i + label + 1
	}

	// Adding the domain terminator "0x00"
	respBuffer = append(respBuffer, msgQuestion[i])
	i++

	// Adding Type
	respBuffer = append(respBuffer, msgQuestion[i:i+2]...)

	// Adding Class
	respBuffer = append(respBuffer, msgQuestion[i+2:i+4]...)

	// Adding pointer label and index
	// See: https://datatracker.ietf.org/doc/html/rfc1035#section-4.1.4
	pointerLabel, _ := hex.DecodeString("C0")
	addrIndex := 12
	respBuffer = append(respBuffer, pointerLabel...)
	respBuffer = append(respBuffer, byte(addrIndex))

	// Adding Type for answer
	respBuffer = append(respBuffer, msgQuestion[i:i+2]...)

	// Adding Class for answer
	respBuffer = append(respBuffer, msgQuestion[i+2:i+4]...)

	// Adding TTL (in seconds)
	ttl := make([]byte, 4)
	// Setting it to 1 hour (3600s)
	binary.BigEndian.PutUint32(ttl, 3600)
	respBuffer = append(respBuffer, ttl...)

	// Adding length of data, since its always ::1 (IPv6) it will be 16 bytes
	// Represented as 0000000000000001
	dataLength := make([]byte, 2)
	binary.BigEndian.PutUint16(dataLength, 16)
	respBuffer = append(respBuffer, dataLength...)
	for j := 0; j < 15; j++ {
		respBuffer = append(respBuffer, byte(0))
	}
	respBuffer = append(respBuffer, byte(1))

	// Writing the response back to UDP connection
	_, err := udpConn.WriteToUDP(respBuffer, udpWriteAddr)
	if err != nil {
		log.Fatal("Failed to write UDP response", err)
	}
}
