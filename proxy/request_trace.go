package proxy

import (
	"errors"
	"fmt"
	"kiro-go/config"
	"strings"
	"sync"
	"time"
)

const slowTTFTDebugThresholdMs int64 = 10_000

var ttftRetryTimeout = 40 * time.Second

type ttftTimeoutError struct {
	Timeout  time.Duration
	Endpoint string
}

func (e *ttftTimeoutError) Error() string {
	if e == nil {
		return "ttft timeout"
	}
	if e.Endpoint != "" {
		return fmt.Sprintf("ttft timeout after %dms waiting for first token on %s", e.Timeout.Milliseconds(), e.Endpoint)
	}
	return fmt.Sprintf("ttft timeout after %dms waiting for first token", e.Timeout.Milliseconds())
}

func isTTFTTimeoutError(err error) bool {
	var target *ttftTimeoutError
	return errors.As(err, &target)
}

func ttftTimeoutFromError(err error) time.Duration {
	var target *ttftTimeoutError
	if errors.As(err, &target) && target != nil {
		return target.Timeout
	}
	return ttftRetryTimeout
}

type requestTraceEvent struct {
	Name       string
	AtMs       int64
	DurationMs int64
	Detail     string
}

type requestTrace struct {
	start  time.Time
	mu     sync.Mutex
	events []requestTraceEvent
}

func newRequestTrace(start time.Time) *requestTrace {
	return &requestTrace{start: start}
}

func (t *requestTrace) mark(name, detail string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.events = append(t.events, requestTraceEvent{
		Name:       cleanTraceValue(name),
		AtMs:       time.Since(t.start).Milliseconds(),
		DurationMs: -1,
		Detail:     cleanTraceValue(detail),
	})
	t.mu.Unlock()
}

func (t *requestTrace) addDuration(name string, started time.Time, detail string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.events = append(t.events, requestTraceEvent{
		Name:       cleanTraceValue(name),
		AtMs:       started.Sub(t.start).Milliseconds(),
		DurationMs: time.Since(started).Milliseconds(),
		Detail:     cleanTraceValue(detail),
	})
	t.mu.Unlock()
}

func (t *requestTrace) summary() string {
	if t == nil {
		return ""
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	parts := make([]string, 0, len(t.events))
	for _, e := range t.events {
		timePart := fmt.Sprintf("%s@%dms", e.Name, e.AtMs)
		if e.DurationMs >= 0 {
			timePart += fmt.Sprintf("+%dms", e.DurationMs)
		}
		if e.Detail != "" {
			timePart += "(" + e.Detail + ")"
		}
		parts = append(parts, timePart)
	}
	return strings.Join(parts, ";")
}

func cleanTraceValue(value string) string {
	value = strings.Join(strings.Fields(value), " ")
	if len(value) > 240 {
		return value[:240] + "..."
	}
	return value
}

func traceAccountID(account *config.Account) string {
	if account == nil {
		return ""
	}
	return account.ID
}

func traceErr(err error) string {
	if err == nil {
		return ""
	}
	return cleanTraceValue(err.Error())
}

func (h *Handler) pickAccountForModelWithTrace(sessionKey, model string, excluded map[string]bool, attempt int, trace *requestTrace) *config.Account {
	started := time.Now()
	var account *config.Account
	if attempt == 0 {
		account = h.pool.GetForSession(sessionKey, model, excluded)
	} else {
		account = h.pool.GetNextForModelExcluding(model, excluded)
	}
	status := "ok"
	if account == nil {
		status = "none"
	}
	trace.addDuration("pick_account", started, fmt.Sprintf("attempt=%d model=%s account=%s status=%s", attempt+1, model, traceAccountID(account), status))
	return account
}

func (h *Handler) ensureValidTokenWithTrace(account *config.Account, trace *requestTrace) error {
	started := time.Now()
	err := h.ensureValidToken(account)
	if err != nil {
		trace.addDuration("ensure_token", started, fmt.Sprintf("account=%s status=error error=%s", traceAccountID(account), traceErr(err)))
		return err
	}
	trace.addDuration("ensure_token", started, fmt.Sprintf("account=%s status=ok", traceAccountID(account)))
	return nil
}
