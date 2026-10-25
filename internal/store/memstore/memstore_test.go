package memstore_test

import (
	"testing"
	"time"

	"github.com/amit/buyer-throttling/internal/clock"
	"github.com/amit/buyer-throttling/internal/store"
	"github.com/amit/buyer-throttling/internal/store/memstore"
	"github.com/amit/buyer-throttling/internal/store/storetest"
)

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) (store.Store, func(time.Duration)) {
		c := clock.NewFake(time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC))
		return memstore.New(c), c.Advance
	})
}
