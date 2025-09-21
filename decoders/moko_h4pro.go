package decoders

import (
	"encoding/hex"
	"fmt"
	"log"
	"strings"
)

// Exported (capital P) and no dependency on main package types:
func ParseH4ProToOutput(frameType byte, p []byte, ts int64, mac, deviceName string, rssi *int) (map[string]any, error) {
	switch frameType {
	case 0x70:
		log.Printf("DEC H4Pro frame=0x70 T&H len=%d", len(p))
		return buildTH(ts, mac, deviceName, rssi, p)
	case 0x40:
		log.Printf("DEC H4Pro frame=0x40 INFO len=%d", len(p))
		return buildInfo(ts, mac, deviceName, rssi, p)
	default:
		return nil, fmt.Errorf("unknown H4 Pro frame_type 0x%02X", frameType)
	}
}

func u16be(b []byte, off *int) (int, bool) {
	if len(b) < *off+2 {
		return 0, false
	}
	v := int(b[*off])<<8 | int(b[*off+1])
	*off += 2
	return v, true
}

func i16be(b []byte, off *int) (int, bool) {
	if len(b) < *off+2 {
		return 0, false
	}
	v := int(int16(b[*off])<<8 | int16(b[*off+1]))
	*off += 2
	return v, true
}

func buildTH(ts int64, mac, deviceName string, rssi *int, p []byte) (map[string]any, error) {
	out := map[string]any{
		"message_type": "h4pro-t&h",
		"timestamp":    ts,
		"mac":          strings.ToUpper(mac),
		"device_name":  deviceName,
	}
	if rssi != nil {
		out["rssi"] = *rssi
	}

	off := 0
	// Ranging (1)
	if len(p) >= off+1 {
		off++
	}
	// Adv interval (1) → guardamos steps y ms
	if len(p) >= off+1 {
		steps := int(p[off])
		off++
		out["adv_interval_steps"] = steps
		out["adv_interval_ms"] = steps * 100 // 1 step = 100 ms
	}

	// Temp i16 (/10)
	if v, ok := i16be(p, &off); ok {
		out["temperature"] = float64(v) / 10.0
	}
	// Humidity u16 (/10)
	if v, ok := u16be(p, &off); ok {
		out["humidity"] = float64(v) / 10.0
	}
	// Battery mV u16
	if v, ok := u16be(p, &off); ok {
		out["batt_vol"] = v
	}

	// Device type (1)
	if len(p) >= off+1 {
		out["device_type"] = int(p[off])
		off++
	}
	// MAC in frame (6) — ignore in output
	if len(p) >= off+6 {
		_ = strings.ToLower(hex.EncodeToString(p[off : off+6]))
		// off += 6
	}
	// If firmware bytes were present here in TH frame variants, parse; otherwise ignore silently.
	if rem := len(p) - off; rem > 0 {
		log.Printf("DEC H4Pro TH trailing_bytes=%d off=%d", rem, off)
		off += 6
	}

	return out, nil
}

func buildInfo(ts int64, mac, deviceName string, rssi *int, p []byte) (map[string]any, error) {
	out := map[string]any{
		"message_type": "h4pro-info",
		"timestamp":    ts,
		"mac":          strings.ToUpper(mac),
		"device_name":  deviceName,
	}
	if rssi != nil {
		out["rssi"] = *rssi
	}

	off := 0
	// Ranging (1)
	if len(p) >= off+1 {
		off++
	}
	// Adv interval (1) → steps y ms
	if len(p) >= off+1 {
		steps := int(p[off])
		off++
		out["adv_interval_steps"] = steps
		out["adv_interval_ms"] = steps * 100 // 1 step = 100 ms
	}
	// Battery mV (u16)
	if v, ok := u16be(p, &off); ok {
		out["batt_vol"] = v
	}
	// Device property indicator (1)
	if len(p) >= off+1 {
		val := int(p[off])
		off++
		out["device_prop"] = val
		out["device_prop_bits"] = fmt.Sprintf("%08b", val)
	}
	// Switch status indicator (1)
	if len(p) >= off+1 {
		val := int(p[off])
		off++
		out["switch_status"] = val
		out["switch_status_bits"] = fmt.Sprintf("%08b", val)
	}
	// MAC en frame (6)
	if len(p) >= off+6 {
		off += 6
	}
	// Firmware version (u16 → V0.0.<n>)
	if v, ok := u16be(p, &off); ok {
		out["firmware_ver"] = fmt.Sprintf("V0.0.%d", v)
	}

	return out, nil
}
