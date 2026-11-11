package worker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestErrors(t *testing.T) {
	cause := errors.New("boom")
	err := Retry("captcha", cause, After(time.Second), BlockQueue(time.Minute))
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("Retry must return an *Error, got %T", err)
	}
	if e.Kind != KindRetry || e.Code != "captcha" || e.RetryAfter != time.Second || e.BlockQueueFor != time.Minute {
		t.Fatalf("retry: %+v", e)
	}
	if !errors.Is(err, cause) || err.Error() != "captcha: boom" {
		t.Fatalf("retry must wrap its cause: %v", err)
	}
	err = Fail("gone", nil)
	if !errors.As(err, &e) || e.Kind != KindFail || err.Error() != "gone" {
		t.Fatalf("fail: %v", err)
	}
}

func TestStrainOption(t *testing.T) {
	var e *Error
	if err := Retry("slow", nil, Strain()); !errors.As(err, &e) || !e.Strain {
		t.Fatalf("Strain must mark the failure: %+v", e)
	}
	if err := Retry("slow", nil); !errors.As(err, &e) || e.Strain {
		t.Fatalf("a failure is no sign of strain unless it says so: %+v", e)
	}
}

// TestReportStrain: the signal goes to the Frontier with a failure, but not with a
// task that is handed back.
func TestReportStrain(t *testing.T) {
	a := &active{leaseID: "l", attempt: 1}
	poolFor := func(string) string { return "" }
	err := Retry("http_503", nil, Strain())
	if rep := buildReport(a, &Task{}, nil, err, "", poolFor); !rep.complete.GetOutcome().GetStrain() {
		t.Error("a failure that signals strain must carry it")
	}
	if rep := buildReport(a, &Task{}, nil, err, codeShutdown, poolFor); rep.complete.GetOutcome().GetStrain() {
		t.Error("a task handed back says nothing about the site")
	}
	if rep := buildReport(a, &Task{}, nil, Retry("x", nil), "", poolFor); rep.complete.GetOutcome().GetStrain() {
		t.Error("strain must be opt-in")
	}
}

func TestClassify(t *testing.T) {
	cases := []struct {
		name string
		err  error
		kind Kind
		code string
	}{
		{"classified", Fail("gone", nil), KindFail, "gone"},
		{"wrapped classified", fmt.Errorf("parse: %w", Fail("layout", nil)), KindFail, "layout"},
		{"deadline", fmt.Errorf("fetch: %w", context.DeadlineExceeded), KindRetry, "timeout"},
		{"plain", errors.New("anything"), KindRetry, "error"},
	}
	for _, c := range cases {
		if e := classify(c.err); e.Kind != c.kind || e.Code != c.code {
			t.Errorf("%s: got kind %d code %q", c.name, e.Kind, e.Code)
		}
	}
	if e := classify(context.DeadlineExceeded); e.Strain {
		t.Error("a handler that ran out of time says nothing about a site")
	}
}

func TestFuncAdapters(t *testing.T) {
	h := HandlerFunc(func(context.Context, *Task) (*Result, error) { return &Result{Items: 3}, nil })
	if res, err := h.Handle(context.Background(), &Task{}); err != nil || res.Items != 3 {
		t.Fatalf("HandlerFunc: %v %v", res, err)
	}
	p := ParserFunc(func(context.Context, *Task, *Response) (*Result, error) { return &Result{Items: 4}, nil })
	if res, err := p.Parse(context.Background(), &Task{}, &Response{}); err != nil || res.Items != 4 {
		t.Fatalf("ParserFunc: %v %v", res, err)
	}
}
