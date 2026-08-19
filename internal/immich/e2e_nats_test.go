//go:build integration

package immich

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/herrfennessey/immich-listener/internal/events"
	natsclient "github.com/nats-io/nats.go"
)

type e2eNATS struct {
	nc  *natsclient.Conn
	sub *natsclient.Subscription
}

func newE2ENATS(t *testing.T, suite *e2eSuite) *e2eNATS {
	t.Helper()
	nc, err := natsclient.Connect(suite.natsURL)
	if err != nil {
		t.Fatalf("connect to NATS: %v", err)
	}
	sub, err := nc.SubscribeSync("immich.>")
	if err != nil {
		nc.Close()
		t.Fatalf("subscribe to listener events: %v", err)
	}
	if err := nc.Flush(); err != nil {
		nc.Close()
		t.Fatalf("flush NATS subscription: %v", err)
	}
	t.Cleanup(nc.Close)
	return &e2eNATS{nc: nc, sub: sub}
}

func (n *e2eNATS) AwaitEvent(t *testing.T, eventType events.Type, matches func(events.Event) bool) events.Event {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("timed out waiting for %s", eventType)
		}
		message, err := n.sub.NextMsg(remaining)
		if err != nil {
			t.Fatalf("read listener event: %v", err)
		}
		var event events.Event
		if err := json.Unmarshal(message.Data, &event); err != nil {
			t.Fatalf("decode listener event: %v", err)
		}
		if event.Type == eventType && matches(event) {
			return event
		}
	}
}
