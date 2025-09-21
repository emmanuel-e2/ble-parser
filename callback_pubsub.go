package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/pubsub/v2"
)

type CallbackEvent struct {
	DeviceId  string         `json:"deviceId"`
	Type      string         `json:"type"`
	Timestamp int64          `json:"timestamp"`
	GatewayID string         `json:"gateway_id,omitempty"`
	Data      map[string]any `json:"data,omitempty"`
	BackendID int64
}

var (
	psClient      *pubsub.Client
	cbPublisher   *pubsub.Publisher
	orderingOn    bool
	callbackTopic string
)

func initPubSub(ctx context.Context) error {
	projectID := os.Getenv("GCP_PROJECT_ID")
	callbackTopic = os.Getenv("CALLBACK_TOPIC")
	if projectID == "" || callbackTopic == "" {
		return fmt.Errorf("missing GCP_PROJECT_ID or CALLBACK_TOPIC env var")
	}

	switch strings.ToLower(os.Getenv("CALLBACK_ORDERING")) {
	case "1", "true", "yes":
		orderingOn = true
	}

	cl, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("pubsub.NewClient: %w", err)
	}
	psClient = cl

	// You can pass the topic ID ("ble-callbacks") or full name.
	pub := cl.Publisher(callbackTopic)
	// Tuning (optional)
	pub.PublishSettings.DelayThreshold = 50 * time.Millisecond
	pub.PublishSettings.Timeout = 10 * time.Second
	pub.EnableMessageOrdering = orderingOn

	cbPublisher = pub
	log.Printf("Pub/Sub v2 initialized: topic=%s ordering=%v", callbackTopic, orderingOn)
	return nil
}

func closePubSub() {
	if cbPublisher != nil {
		cbPublisher.Stop()
	}
	if psClient != nil {
		_ = psClient.Close()
	}
}

func publishCallback(ctx context.Context, evt CallbackEvent) error {
	if evt.Timestamp == 0 {
		evt.Timestamp = time.Now().UnixMilli()
	}
	evt.DeviceId = strings.ToUpper(evt.DeviceId)

	b, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal callback event: %w", err)
	}

	msg := &pubsub.Message{
		Data: b,
		Attributes: map[string]string{
			"source":     "ble-parser",
			"type":       evt.Type,
			"deviceId":   evt.DeviceId,
			"gateway_id": evt.GatewayID,
		},
	}
	if orderingOn {
		// Per-device ordering (requires subscription has ordering enabled)
		msg.OrderingKey = evt.DeviceId
	}
	id := ""
	err = nil
	res := cbPublisher.Publish(ctx, msg)
	if id, err = res.Get(ctx); err != nil {
		return fmt.Errorf("publish failed: %w", err)
	}
	log.Printf("publishCallback ok topic=%s id=%s bytes=%d", callbackTopic, id, len(b))
	return nil
}
