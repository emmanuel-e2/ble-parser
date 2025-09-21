package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"strings"
)

// extractServiceData16 finds AD type 0x16 (Service Data - 16-bit UUID).
// Returns UUID text (e.g., "FEAB"), frameType, remaining service data bytes, ok.
func extractServiceData16(payloadHex string) (uuidText string, frameType byte, rest []byte, ok bool) {
	raw, err := hex.DecodeString(strings.TrimSpace(payloadHex))
	if err != nil || len(raw) == 0 {
		return "", 0, nil, false
	}
	i := 0
	adIdx := 0
	for i < len(raw) {
		if i+1 >= len(raw) {
			return "", 0, nil, false
		}
		length := int(raw[i]) // includes type + data
		if length == 0 || i+1+length > len(raw) {
			log.Printf("AD %d malformed length=%d at i=%d total=%d", adIdx, length, i, len(raw))
			return "", 0, nil, false
		}
		typ := raw[i+1]
		// Debug each AD structure
		log.Printf("AD %d len=%d type=0x%02X", adIdx, length, typ)
		if typ == 0x16 {
			if length < 4 {
				log.Printf("AD %d type=0x16 too short length=%d", adIdx, length)
				return "", 0, nil, false
			}
			lsb := raw[i+2] // least significant byte first in AD
			msb := raw[i+3]
			uuidText = strings.ToUpper(fmt.Sprintf("%02X%02X", msb, lsb))
			data := raw[i+4 : i+1+length]
			if len(data) == 0 {
				return uuidText, 0, nil, true
			}
			frameType = data[0]
			if len(data) > 1 {
				rest = data[1:]
			} else {
				rest = nil
			}
			return uuidText, frameType, rest, true
		}
		i += 1 + length
		adIdx++
	}
	return "", 0, nil, false
}
