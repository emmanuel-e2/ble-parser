package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"strings"
	"time"
	"unicode"

	dec "ble-parser/decoders" // <— ADD: import your subpackage

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var db *pgxpool.Pool

type MQTTMessage struct {
	MessageID  int64  `json:"message_id"`
	GatewayMAC string `json:"gateway_mac"`
	GatewayHW  string `json:"gateway_hw"`
	DeviceMAC  string `json:"device_mac"`
	Payload    string `json:"payload"`
	QoS        int    `json:"qos"`
	Timestamp  int64  `json:"timestamp"`
	RSSI       *int   `json:"rssi,omitempty"`
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	ctx := context.Background()

	if err := connectDB(); err != nil {
		log.Fatalf("db connect: %v", err)
	}

	if err := initPubSub(ctx); err != nil {
		log.Fatalf("initPubSub: %v", err)
	}
	defer closePubSub()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("parser ok"))
	})
	mux.HandleFunc("/message", handleMessage)
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("parser"))
	})

	addr := ":" + getenv("HTTPPORT", "8080")
	log.Printf("parser listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil && err != http.ErrServerClosed {
		log.Fatalf("ListenAndServe: %v", err)
	}
}

func handleMessage(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	trace := genTraceID()

	if r.Method != http.MethodPost {
		http.Error(w, "only POST", http.StatusMethodNotAllowed)
		return
	}

	var in MQTTMessage
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		log.Printf("MSG %s decode error: %v", trace, err)
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}

	normalize(&in)
	log.Printf("MSG %s recv msg_id=%d gw_mac=%s gw_hw=%s dev_mac=%s qos=%d ts=%d rssi=%v",
		trace, in.MessageID, in.GatewayMAC, in.GatewayHW, in.DeviceMAC, in.QoS, in.Timestamp, ptrIntStr(in.RSSI))

	if err := validate(&in); err != nil {
		log.Printf("MSG %s validation error: %v", trace, err)
		http.Error(w, "validation: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Quick payload preview for logs
	pl := strings.TrimSpace(in.Payload)
	log.Printf("MSG %s payload.len=%d preview=%q", trace, len(pl), head(pl, getenvInt("LOG_PAYLOAD_PREVIEW_CHARS", 32)))

	// Validate hex
	if _, err := hex.DecodeString(pl); err != nil {
		log.Printf("MSG %s invalid hex payload: %v", trace, err)
		http.Error(w, "payload must be hex: "+err.Error(), http.StatusBadRequest)
		return
	}

	// 2) Gateway enrichment (not used yet → assign to _ to avoid compile error)
	gwName, gwHW, gwClient := fetchGateway(in.GatewayMAC)
	if gwName == "" && gwHW == "" {
		log.Printf("MSG %s gateway not found gw_mac=%s", trace, in.GatewayMAC)
	} else {
		log.Printf("MSG %s gateway ok name=%q hw=%q client_id=%q", trace, gwName, gwHW, gwClient)
	}

	// 3) Device enrichment (we only use name & hw_type here)
	devName, _, devHW := fetchDevice(in.DeviceMAC)
	if devHW == "" {
		log.Printf("MSG %s device not found dev_mac=%s", trace, in.DeviceMAC)
		http.Error(w, "unknown device_hw_type; device not found", http.StatusBadRequest)
		return
	}
	log.Printf("MSG %s device ok name=%q hw=%q", trace, devName, devHW)

	// 4) Extract Service Data (must be 0x16 with UUID FEAB)
	uuidText, frameType, rest, ok := extractServiceData16(in.Payload)
	if !ok {
		log.Printf("MSG %s no 0x16 service data in ADV", trace)
		http.Error(w, "no 0x16 service data in advertisement", http.StatusBadRequest)
		return
	}

	if strings.ToUpper(uuidText) != "FEAB" {
		msg := fmt.Sprintf("unsupported service uuid %s (need FEAB)", uuidText)
		log.Printf("MSG %s %s", trace, msg)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	// 5) Parse by device_hw_type + frame_type
	var out any
	var err error
	switch devHW {
	case "H4 Pro":
		// NOTE: pass primitives; subpackage cannot use type from main
		log.Printf("MSG %s decode enter hw=%q frame=0x%02X", trace, devHW, frameType)
		out, err = dec.ParseH4ProToOutput(frameType, rest, in.Timestamp, in.DeviceMAC, devName, in.RSSI)
	default:
		err = fmt.Errorf("no parser for device_hw_type=%q", devHW)
	}
	if err != nil {
		log.Printf("MSG %s parse error: %v", trace, err)
		http.Error(w, "parse error: "+err.Error(), http.StatusBadRequest)
		return
	}
	log.Printf("MSG %s decode ok", trace)

	// 6) Store the output JSON into backend_message.parsed_json
	if err := updateParsedJSON(in.MessageID, out); err != nil {
		var pgErr *pgconn.PgError
		if ok := errorAs(err, &pgErr); ok {
			log.Printf("MSG %s db update error: %s (%s) detail=%s", trace, pgErr.Message, pgErr.Code, pgErr.Detail)
		} else {
			log.Printf("MSG %s db update error: %v", trace, err)
		}
		http.Error(w, "db update parsed_json: "+err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("MSG %s db update ok message_id=%d", trace, in.MessageID)

	// 7) Publish to Pub/Sub callbacks (if Pub/Sub v2 publisher was initialized)
	if cbPublisher != nil {
		// Optional RSSI insertion (in.RSSI is *int)
		var rssiVal any
		if in.RSSI != nil {
			rssiVal = *in.RSSI
		}

		evt := CallbackEvent{
			DeviceId:  strings.ToUpper(in.DeviceMAC),
			Type:      deriveEventType(devHW, frameType, out),
			Timestamp: in.Timestamp,
			GatewayID: strings.ToUpper(in.GatewayMAC),
			Data: map[string]any{
				"parsed_json": out,
				"raw_data":    in.Payload,
				"uuid":        strings.ToUpper(uuidText),
				"frame_type":  fmt.Sprintf("0x%02X", frameType),
			},
			BackendID: in.MessageID,
		}
		if rssiVal != nil {
			evt.Data["rssi"] = rssiVal
		}

		if err := publishCallback(r.Context(), evt); err != nil {
			log.Printf("MSG %s publishCallback error: %v", trace, err)
			// non fatal, we already stored parsed_json
		} else {
			log.Printf("MSG %s publishCallback ok topic=%s, device=%s", trace, callbackTopic, evt.DeviceId)
		}
	} else {
		log.Printf("MSG %s pubsub not initialized; skipping publish", trace)
	}

	elapsed := time.Since(start).Milliseconds()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":         "ok",
		"message_id":     in.MessageID,
		"device_hw_type": devHW,
		"frame_type":     fmt.Sprintf("0x%02X", frameType),
		"ms":             elapsed,
	})
}

func normalize(m *MQTTMessage) {
	m.DeviceMAC = strings.ToLower(strings.NewReplacer(":", "", "-", "", ".", "", " ", "").Replace(m.DeviceMAC))
	m.GatewayMAC = strings.ToUpper(strings.NewReplacer(":", "", "-", "", ".", "", " ", "").Replace(m.GatewayMAC))
	m.Payload = strings.TrimSpace(m.Payload)
}

func validate(m *MQTTMessage) error {
	if m.MessageID <= 0 {
		return fmt.Errorf("message_id must be > 0")
	}
	if m.DeviceMAC == "" {
		return fmt.Errorf("device_mac required")
	}
	if m.Payload == "" {
		return fmt.Errorf("payload empty")
	}
	if m.Timestamp <= 0 {
		return fmt.Errorf("timestamp ms required")
	}
	// hex sanity (fast path)
	if !isLikelyHex(m.Payload) {
		return fmt.Errorf("payload is not hex-like")
	}
	return nil
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func getenvInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := fmt.Sscanf(v, "%d", &def); err == nil && n == 1 {
			return def
		}
	}
	return def
}

func head(s string, n int) string {
	if n <= 0 || len(s) <= n {
		return s
	}
	return s[:n]
}

func isLikelyHex(s string) bool {
	if len(s) == 0 || (len(s)%2) != 0 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}
func ptrIntStr(p *int) string {
	if p == nil {
		return "nil"
	}
	return fmt.Sprintf("%d", *p)
}
func genTraceID() string {
	return fmt.Sprintf("%08x", uint32(rand.Uint32()))
}

// errorAs is a tiny local helper to avoid importing errors for a single use
func errorAs(err error, target interface{}) bool {
	switch t := target.(type) {
	case **pgconn.PgError:
		if e, ok := err.(*pgconn.PgError); ok {
			*t = e
			return true
		}
	}
	return false
}

func deriveEventType(devHW string, frameType byte, parsed any) string {
	// If parser already produced a type string, prefer it
	if m, ok := parsed.(map[string]any); ok {
		if v, ok := m["type"].(string); ok && v != "" {
			return v
		}
		if v, ok := m["message_type"].(string); ok && v != "" {
			return v
		}
		if v, ok := m["messageType"].(string); ok && v != "" {
			return v
		}
	}
	// Fallback: device family slug + frame code
	return fmt.Sprintf("%s/0x%02X", slugDeviceFamily(devHW), frameType)
}

func slugDeviceFamily(s string) string {
	// minimal, predictable slug for device family (e.g., "H4 Pro" -> "h4-pro")
	b := make([]rune, 0, len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b = append(b, unicode.ToLower(r))
		case r == ' ' || r == '_' || r == '-' || r == '/':
			if len(b) == 0 || b[len(b)-1] == '-' {
				continue
			}
			b = append(b, '-')
		default:
			// drop other punctuation
		}
	}
	// trim trailing '-'
	if len(b) > 0 && b[len(b)-1] == '-' {
		b = b[:len(b)-1]
	}
	if len(b) == 0 {
		return "unknown"
	}
	return string(b)
}
