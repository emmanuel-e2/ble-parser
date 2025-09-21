package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/cloudsqlconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var pg *pgxpool.Pool

func connectDB() error {
	dbUser := os.Getenv("DB_USER")
	dbPass := os.Getenv("DB_PASSWORD")
	dbName := os.Getenv("DB_NAME")
	instance := os.Getenv("INSTANCE_CONNECTION_NAME")
	usePrivate := os.Getenv("PRIVATE_IP") != ""

	if dbUser == "" || dbPass == "" || dbName == "" || instance == "" {
		return fmt.Errorf("missing DB envs (DB_USER/DB_PASSWORD/DB_NAME/INSTANCE_CONNECTION_NAME)")
	}

	dsn := fmt.Sprintf("user=%s password=%s database=%s sslmode=disable", dbUser, dbPass, dbName)

	opts := []cloudsqlconn.Option{}
	if usePrivate {
		opts = append(opts, cloudsqlconn.WithDefaultDialOptions(cloudsqlconn.WithPrivateIP()))
	}
	d, err := cloudsqlconn.NewDialer(context.Background(), opts...)
	if err != nil {
		return fmt.Errorf("cloudsql dialer: %w", err)
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("pgxpool.ParseConfig: %w", err)
	}
	cfg.ConnConfig.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		return d.Dial(ctx, instance)
	}
	cfg.MinConns = 0
	cfg.MaxConns = 10
	cfg.MaxConnIdleTime = 5 * time.Minute

	pg, err = pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return fmt.Errorf("pgxpool.NewWithConfig: %w", err)
	}
	if err := pg.Ping(context.Background()); err != nil {
		return fmt.Errorf("db ping: %w", err)
	}
	log.Println("CONNECTED TO DATABASE")
	return nil
}

// macHexToBytea converts a mac in hex (with or withour separator) to raw 6 bytes.
func macHexToBytea(s string) ([]byte, error) {
	s = strings.TrimSpace(s)
	// Accept forms like "AA:BB:CC:DD:EE:FF", "aa-bb-...", "aabbccddeeff"
	s = strings.NewReplacer(":", "", "-", "", ".", "", " ", "").Replace(s)
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("Invalid hex mac %q: %w", s, err)
	}
	if len(b) != 6 {
		return nil, fmt.Errorf("mac must be 6 bytes, got %d", len(b))
	}
	return b, nil
}

func fetchGateway(mac string) (name, hwType, clientID string) {
	ctx := context.Background()
	bmac, err := macHexToBytea(mac)
	if err != nil {
		log.Printf("fetchGateway: %v", err)
		return
	}
	row := pg.QueryRow(ctx,
		`SELECT gateway_name, gateway_hw_type, client_id
			FROM gateways
			WHERE gateway_mac = $1`, bmac)
	_ = row.Scan(&name, &hwType, &clientID)
	return
}

func fetchDevice(mac string) (name, deviceID, hwType string) {
	ctx := context.Background()
	bmac, err := macHexToBytea(mac)
	if err != nil {
		log.Printf("fetchDevice: %v", err)
		return
	}
	row := pg.QueryRow(ctx,
		`SELECT device_name, device_id, device_hw_type
			FROM devices
			WHERE device_mac = $1`, bmac)
	_ = row.Scan(&name, &deviceID, &hwType)
	return
}

// Update parsed_json for the existing backend_message row (id == message_id)
func updateParsedJSON(backendID int64, v any) error {
	ctx := context.Background()
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	ct, err := pg.Exec(ctx, `UPDATE backend_message SET parser_json = $2 WHERE id = $1`, backendID, b)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return fmt.Errorf("no backend_message row found for id=%d", backendID)
	}
	return nil
}
