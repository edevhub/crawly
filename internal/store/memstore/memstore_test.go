package memstore_test

import (
	"testing"
	"time"

	"github.com/edevhub/crawly/internal/store"
	"github.com/edevhub/crawly/internal/store/memstore"
	"github.com/edevhub/crawly/internal/store/storetest"
)

func TestConformance(t *testing.T) {
	storetest.Run(t, func(pol store.Policies, now func() time.Time) store.Store {
		return memstore.New(pol, memstore.WithClock(now))
	})
}
