package prom_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/edevhub/crawly/sdk/worker"
	"github.com/edevhub/crawly/sdk/worker/prom"
)

func TestNewRegistersOnTheGivenRegistryOnly(t *testing.T) {
	reg := prometheus.NewRegistry()
	if _, err := prom.New(reg); err != nil {
		t.Fatal(err)
	}
	if _, err := prom.New(reg); err == nil {
		t.Error("a second worker on one registry must be refused, not silently merged")
	}
	other := prometheus.NewRegistry()
	for _, id := range []string{"1", "2"} {
		if _, err := prom.New(prometheus.WrapRegistererWith(prometheus.Labels{"worker": id}, other)); err != nil {
			t.Errorf("workers on wrapped registries with their own labels must share a registry: %v", err)
		}
	}
}

func TestCodesAreBounded(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := prom.New(reg)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 1000 {
		m.TaskStarted("default", "page")
		m.TaskFinished(worker.TaskDone{Pool: "default", Handler: "page", Kind: "retry", Code: fmt.Sprintf("code_%d", i), Duration: time.Millisecond})
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() == "crawly_worker_tasks_finished_total" && len(mf.GetMetric()) != 101 {
			t.Errorf("%d series for 1000 codes, want the cap and one for the rest", len(mf.GetMetric()))
		}
		if mf.GetName() == "crawly_worker_tasks_running" && mf.GetMetric()[0].GetGauge().GetValue() != 0 {
			t.Errorf("handlers running: %v", mf.GetMetric()[0].GetGauge().GetValue())
		}
	}
}
