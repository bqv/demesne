package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAReservationNeedsADeviceAndBytes(t *testing.T) {
	cfg := testConfig(t)
	d := newTestDemesne(t, cfg)
	for _, body := range []string{
		`{"bytes":1073741824}`,
		`{"bytes":1073741824,"device":"CPU"}`,
		`{"bytes":1073741824,"device":"RX7900","label":"x","ttl_s":0}` + "\x00",
	} {
		rec := httptest.NewRecorder()
		d.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/reserve", strings.NewReader(body)))
		if rec.Code == http.StatusOK {
			continue
		}
		if rec.Code != http.StatusBadRequest && rec.Code != http.StatusConflict {
			t.Fatalf("body %q answered %d: %s", body, rec.Code, rec.Body.String())
		}
	}
}

func TestAReservationExpiresAndCanBeReleased(t *testing.T) {
	b := newReserveBoard()
	now := time.Now().UnixMilli()
	b.add(&reservation{ID: "rsv-aa", Device: "GT1030", Bytes: 1 << 30, Label: "bench",
		ExpiresMS: now + 50})
	b.add(&reservation{ID: "rsv-bb", Device: "GT1030", Bytes: 2 << 30, Label: "other"})
	if got := b.total("GT1030"); got != 3<<30 {
		t.Fatalf("total = %d", got)
	}
	time.Sleep(120 * time.Millisecond)
	if got := b.total("GT1030"); got != 2<<30 {
		t.Fatalf("an expired reservation still counts: %d", got)
	}
	if !b.release("rsv-bb") || b.total("GT1030") != 0 {
		t.Fatal("release did not free the reservation")
	}
	if b.release("rsv-bb") {
		t.Fatal("releasing twice reported success")
	}
}

func TestReservationsArePerCard(t *testing.T) {
	b := newReserveBoard()
	b.add(&reservation{ID: "rsv-x", Device: "RX7900", Bytes: 8 << 30, Label: "ppl"})
	if b.total("GT1030") != 0 {
		t.Fatal("a 7900 reservation counted against the 1030")
	}
	if b.total("RX7900") != 8<<30 {
		t.Fatal("the 7900 reservation was not counted")
	}
}
