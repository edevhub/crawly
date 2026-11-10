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
