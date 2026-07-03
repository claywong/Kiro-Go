package pool

import (
	"errors"
	"kiro-go/config"
	"path/filepath"
	"testing"
	"time"
)

func TestOverLimitAccountsAreSkippedByDefault(t *testing.T) {
	p := &AccountPool{}
	normal := config.Account{ID: "normal"}
	overLimit := config.Account{ID: "over", UsageCurrent: 10, UsageLimit: 10}

	p.accounts = []config.Account{normal, overLimit}

	for i := 0; i < 5; i++ {
		acc := p.GetNext()
		if acc == nil {
			t.Fatalf("expected an account")
		}
		if acc.ID == "over" {
			t.Fatalf("expected over-limit account to be skipped when upstream OverageStatus is empty")
		}
	}
}

func TestOverLimitAccountsCanBeSelectedWhenUpstreamOverageEnabled(t *testing.T) {
	p := &AccountPool{}
	overLimit := config.Account{
		ID:            "over",
		UsageCurrent:  10,
		UsageLimit:    10,
		OverageStatus: "ENABLED",
	}

	p.accounts = []config.Account{overLimit}

	acc := p.GetNext()
	if acc == nil {
		t.Fatalf("expected upstream-enabled overage account to be selectable")
	}
	if acc.ID != "over" {
		t.Fatalf("expected overage account, got %q", acc.ID)
	}
}

func TestOverLimitAccountsRemainSkippedWhenUpstreamOverageDisabled(t *testing.T) {
	p := &AccountPool{}
	overLimit := config.Account{
		ID:            "over",
		UsageCurrent:  10,
		UsageLimit:    10,
		OverageStatus: "DISABLED",
	}

	p.accounts = []config.Account{overLimit}

	if acc := p.GetNext(); acc != nil {
		t.Fatalf("expected nil when upstream OverageStatus=DISABLED, got %q", acc.ID)
	}
}

func TestGetNextKeepsFiveMinuteTokenAvailable(t *testing.T) {
	p := &AccountPool{}
	account := config.Account{
		ID:          "acct-1",
		AccessToken: "access-token",
		ExpiresAt:   time.Now().Unix() + 300,
	}

	p.accounts = []config.Account{account}

	got := p.GetNext()
	if got == nil {
		t.Fatalf("expected five-minute token to be available")
	}
	if got.ID != account.ID {
		t.Fatalf("expected account %q, got %q", account.ID, got.ID)
	}
}

// ---------------------------------------------------------------------------
// IsAuthFailure
// ---------------------------------------------------------------------------

func TestIsAuthFailureRecognizes401And403(t *testing.T) {
	positives := []string{
		"HTTP 401 from server",
		"received 403 Forbidden",
		"bad credentials",
		"invalid_grant",
		"invalid_token",
		"token expired",
		"token has expired",
		"unauthorized",
	}
	for _, msg := range positives {
		if !IsAuthFailure(errors.New(msg)) {
			t.Errorf("IsAuthFailure(%q) = false, want true", msg)
		}
	}
}

func TestIsAuthFailureIgnoresFalsePositives(t *testing.T) {
	// hasStatusToken only excludes digit boundaries; e.g. "4011" contains "401"
	// but the trailing '1' is a digit so it does NOT match.
	negatives := []string{
		"status code 4011 found", // digit immediately after 401 → not a standalone token
		"error 14013 exceeded",   // digit before and after 401
		"some random error",
		"status 200 OK",
	}
	for _, msg := range negatives {
		if IsAuthFailure(errors.New(msg)) {
			t.Errorf("IsAuthFailure(%q) = true, want false", msg)
		}
	}
}

func TestIsAuthFailureNilError(t *testing.T) {
	if IsAuthFailure(nil) {
		t.Fatal("IsAuthFailure(nil) = true, want false")
	}
}

// ---------------------------------------------------------------------------
// IsSuspensionError
// ---------------------------------------------------------------------------

func TestIsSuspensionErrorDetectsKnownMessages(t *testing.T) {
	positives := []string{
		"account temporarily_suspended",
		"account temporarily suspended",
		"no available kiro profile",
		"No Available Kiro Profile", // case-insensitive
	}
	for _, msg := range positives {
		if !IsSuspensionError(errors.New(msg)) {
			t.Errorf("IsSuspensionError(%q) = false, want true", msg)
		}
	}
}

func TestIsSuspensionErrorIgnoresUnrelatedErrors(t *testing.T) {
	negatives := []string{
		"some other error",
		"unauthorized",
		"429 too many requests",
	}
	for _, msg := range negatives {
		if IsSuspensionError(errors.New(msg)) {
			t.Errorf("IsSuspensionError(%q) = true, want false", msg)
		}
	}
}

func TestIsSuspensionErrorNilError(t *testing.T) {
	if IsSuspensionError(nil) {
		t.Fatal("IsSuspensionError(nil) = true, want false")
	}
}

// ---------------------------------------------------------------------------
// GetNextForModelExcluding
// ---------------------------------------------------------------------------

func newTestPool(accounts ...config.Account) *AccountPool {
	p := &AccountPool{
		cooldowns:   make(map[string]time.Time),
		errorCounts: make(map[string]int),
		modelLists:  make(map[string]map[string]bool),
		lastUsedAt:  make(map[string]time.Time),
	}
	p.accounts = accounts
	return p
}

func TestMinIntervalThrottlesConsecutivePicks(t *testing.T) {
	p := newTestPool(
		config.Account{ID: "sensitive", MinIntervalMs: 60000},
		config.Account{ID: "normal"},
	)

	picked := map[string]int{}
	for i := 0; i < 4; i++ {
		acc := p.GetNext()
		if acc == nil {
			t.Fatalf("pick %d: expected an account, got nil", i)
		}
		picked[acc.ID]++
	}

	if picked["sensitive"] > 1 {
		t.Fatalf("sensitive account picked %d times within its throttle window, want at most 1", picked["sensitive"])
	}
	if picked["normal"] < 3 {
		t.Fatalf("normal account picked %d times, want at least 3", picked["normal"])
	}
}

func TestMinIntervalAccountAvailableAfterWindow(t *testing.T) {
	p := newTestPool(config.Account{ID: "sensitive", MinIntervalMs: 60000})

	if acc := p.GetNext(); acc == nil || acc.ID != "sensitive" {
		t.Fatalf("first pick: expected sensitive account, got %#v", acc)
	}
	if acc := p.GetNext(); acc != nil {
		t.Fatalf("second pick inside window: expected nil, got %q", acc.ID)
	}

	// 手动把窗口拨到过去，账号应重新可用。
	p.lastMu.Lock()
	p.lastUsedAt["sensitive"] = time.Now().Add(-61 * time.Second)
	p.lastMu.Unlock()

	if acc := p.GetNext(); acc == nil || acc.ID != "sensitive" {
		t.Fatalf("pick after window: expected sensitive account, got %#v", acc)
	}
}

func TestQuotaErrorUsesShortCooldown(t *testing.T) {
	p := newTestPool(config.Account{ID: "a"})
	p.RecordError("a", true)

	cd, ok := p.cooldowns["a"]
	if !ok {
		t.Fatal("expected cooldown to be set after quota error")
	}
	remaining := time.Until(cd)
	if remaining > quotaCooldown || remaining <= 0 {
		t.Fatalf("quota cooldown = %v, want (0, %v]", remaining, quotaCooldown)
	}
}

func TestGetNextForModelExcludingSkipsExcludedAccounts(t *testing.T) {
	p := newTestPool(
		config.Account{ID: "a"},
		config.Account{ID: "b"},
	)
	excluded := map[string]bool{"a": true}
	for i := 0; i < 5; i++ {
		acc := p.GetNextForModelExcluding("model", excluded)
		if acc == nil {
			t.Fatal("expected account b, got nil")
		}
		if acc.ID == "a" {
			t.Fatalf("excluded account a was returned on iteration %d", i)
		}
	}
}

func TestGetNextForModelExcludingReturnsNilWhenAllExcluded(t *testing.T) {
	p := newTestPool(config.Account{ID: "only"})
	acc := p.GetNextForModelExcluding("model", map[string]bool{"only": true})
	if acc != nil {
		t.Fatalf("expected nil when only account is excluded, got %q", acc.ID)
	}
}

func TestGetNextForModelExcludingReturnsNilOnEmptyPool(t *testing.T) {
	p := newTestPool()
	acc := p.GetNextForModelExcluding("model", map[string]bool{})
	if acc != nil {
		t.Fatalf("expected nil for empty pool, got %q", acc.ID)
	}
}

// ---------------------------------------------------------------------------
// DisableAccount
// ---------------------------------------------------------------------------

func TestDisableAccountSetsCooldown(t *testing.T) {
	// Initialize a temporary config so SetAccountBanStatus can persist safely.
	cfgFile := filepath.Join(t.TempDir(), "config.json")
	if err := config.Init(cfgFile); err != nil {
		t.Fatalf("config.Init: %v", err)
	}

	p := newTestPool()
	p.DisableAccount("test-id", "test reason")

	p.mu.RLock()
	cooldown, ok := p.cooldowns["test-id"]
	p.mu.RUnlock()

	if !ok {
		t.Fatal("expected cooldown to be set after DisableAccount")
	}
	// Safety-net cooldown must be at least 23 hours from now.
	minExpected := time.Now().Add(23 * time.Hour)
	if cooldown.Before(minExpected) {
		t.Fatalf("expected cooldown >= 23h in future, got %v", cooldown)
	}
}

func TestGetNextExcludingSkipsExcludedAccount(t *testing.T) {
	p := &AccountPool{
		accounts: []config.Account{
			{ID: "a", Enabled: true},
			{ID: "b", Enabled: true},
		},
		cooldowns:    make(map[string]time.Time),
		errorCounts:  make(map[string]int),
		modelLists:   make(map[string]map[string]bool),
		currentIndex: ^uint64(0),
	}

	acc := p.GetNextExcluding(map[string]bool{"a": true})
	if acc == nil || acc.ID != "b" {
		t.Fatalf("expected account b, got %#v", acc)
	}
}

func TestGetNextForModelExcludingSkipsExcludedAccount(t *testing.T) {
	p := &AccountPool{
		accounts: []config.Account{
			{ID: "a", Enabled: true},
			{ID: "b", Enabled: true},
		},
		cooldowns:    make(map[string]time.Time),
		errorCounts:  make(map[string]int),
		modelLists:   make(map[string]map[string]bool),
		currentIndex: ^uint64(0),
	}
	p.SetModelList("a", []string{"claude-sonnet-4.5"})
	p.SetModelList("b", []string{"claude-sonnet-4.5"})

	acc := p.GetNextForModelExcluding("claude-sonnet-4.5", map[string]bool{"a": true})
	if acc == nil || acc.ID != "b" {
		t.Fatalf("expected account b, got %#v", acc)
	}
}

// ---------------------------------------------------------------------------
// Sticky session routing
// ---------------------------------------------------------------------------

func newStickyTestPool(accounts ...config.Account) *AccountPool {
	p := newTestPool(accounts...)
	p.stickySessions = make(map[string]stickyEntry)
	p.stickyTTL = time.Hour
	return p
}

func TestGetForSessionReusesLastSuccessfulAccount(t *testing.T) {
	p := newStickyTestPool(
		config.Account{ID: "a"},
		config.Account{ID: "b"},
	)
	p.RecordStickySuccess("conv-1", "b")

	for i := 0; i < 5; i++ {
		acc := p.GetForSession("conv-1", "model", map[string]bool{})
		if acc == nil || acc.ID != "b" {
			t.Fatalf("expected sticky account b, got %#v", acc)
		}
	}
}

func TestGetForSessionFallsBackWhenNoStickyEntry(t *testing.T) {
	p := newStickyTestPool(config.Account{ID: "a"})
	acc := p.GetForSession("conv-unseen", "model", map[string]bool{})
	if acc == nil || acc.ID != "a" {
		t.Fatalf("expected fallback to round robin, got %#v", acc)
	}
}

func TestGetForSessionFallsBackWhenStickyAccountExcluded(t *testing.T) {
	p := newStickyTestPool(
		config.Account{ID: "a"},
		config.Account{ID: "b"},
	)
	p.RecordStickySuccess("conv-1", "a")

	acc := p.GetForSession("conv-1", "model", map[string]bool{"a": true})
	if acc == nil || acc.ID != "b" {
		t.Fatalf("expected fallback to account b when sticky account excluded, got %#v", acc)
	}
}

func TestGetForSessionFallsBackWhenStickyEntryExpired(t *testing.T) {
	p := newStickyTestPool(
		config.Account{ID: "a"},
		config.Account{ID: "b"},
	)
	p.mu.Lock()
	p.stickySessions["conv-1"] = stickyEntry{AccountID: "a", ExpiresAt: time.Now().Add(-time.Minute)}
	p.mu.Unlock()

	acc := p.GetForSession("conv-1", "model", map[string]bool{})
	if acc == nil {
		t.Fatal("expected fallback account, got nil")
	}
	if acc.ID == "a" {
		t.Fatal("expected expired sticky entry to be ignored, but it was still used")
	}
}

func TestGetForSessionFallsBackWhenStickyAccountInCooldown(t *testing.T) {
	p := newStickyTestPool(
		config.Account{ID: "a"},
		config.Account{ID: "b"},
	)
	p.RecordStickySuccess("conv-1", "a")
	p.mu.Lock()
	p.cooldowns["a"] = time.Now().Add(time.Minute)
	p.mu.Unlock()

	acc := p.GetForSession("conv-1", "model", map[string]bool{})
	if acc == nil || acc.ID != "b" {
		t.Fatalf("expected fallback to account b when sticky account is in cooldown, got %#v", acc)
	}
}

func TestGetForSessionIgnoresEmptySessionKey(t *testing.T) {
	p := newStickyTestPool(config.Account{ID: "a"})
	acc := p.GetForSession("", "model", map[string]bool{})
	if acc == nil || acc.ID != "a" {
		t.Fatalf("expected round robin when sessionKey is empty, got %#v", acc)
	}
}

func TestGetForSessionDisabledSwitchFallsBackToRoundRobin(t *testing.T) {
	cfgFile := filepath.Join(t.TempDir(), "config.json")
	if err := config.Init(cfgFile); err != nil {
		t.Fatalf("config.Init: %v", err)
	}
	if err := config.UpdateStickySessionRouting(false); err != nil {
		t.Fatalf("UpdateStickySessionRouting: %v", err)
	}

	p := newStickyTestPool(
		config.Account{ID: "a"},
		config.Account{ID: "b"},
	)
	p.RecordStickySuccess("conv-1", "b")

	// RecordStickySuccess itself is a no-op while disabled, so nothing should
	// have been recorded; GetForSession should degrade to plain round robin.
	p.mu.RLock()
	_, tracked := p.stickySessions["conv-1"]
	p.mu.RUnlock()
	if tracked {
		t.Fatal("expected RecordStickySuccess to be a no-op when routing is disabled")
	}

	acc := p.GetForSession("conv-1", "model", map[string]bool{})
	if acc == nil {
		t.Fatal("expected an account from round robin fallback")
	}
}

func TestPruneExpiredStickyLockedRemovesOnlyExpiredEntries(t *testing.T) {
	p := newStickyTestPool()
	now := time.Now()
	p.mu.Lock()
	p.stickySessions["expired"] = stickyEntry{AccountID: "a", ExpiresAt: now.Add(-time.Minute)}
	p.stickySessions["active"] = stickyEntry{AccountID: "b", ExpiresAt: now.Add(time.Minute)}
	p.pruneExpiredStickyLocked(now)
	_, expiredStillPresent := p.stickySessions["expired"]
	_, activeStillPresent := p.stickySessions["active"]
	p.mu.Unlock()

	if expiredStillPresent {
		t.Fatal("expected expired sticky entry to be pruned")
	}
	if !activeStillPresent {
		t.Fatal("expected active sticky entry to remain")
	}
}

// ---------------------------------------------------------------------------
// Reload over-usage filtering
// ---------------------------------------------------------------------------

func TestReloadKeepsOverQuotaAccountWhenAllowOverUsage(t *testing.T) {
	cfgFile := filepath.Join(t.TempDir(), "config.json")
	if err := config.Init(cfgFile); err != nil {
		t.Fatalf("config.Init: %v", err)
	}
	if err := config.AddAccount(config.Account{
		ID:           "over",
		Enabled:      true,
		UsageCurrent: 10,
		UsageLimit:   10,
	}); err != nil {
		t.Fatalf("AddAccount: %v", err)
	}
	if err := config.UpdateAllowOverUsage(true); err != nil {
		t.Fatalf("UpdateAllowOverUsage: %v", err)
	}

	p := newTestPool()
	p.Reload()

	if got := p.GetNext(); got == nil || got.ID != "over" {
		t.Fatalf("expected over-quota account to remain routable when allowOverUsage=true, got %#v", got)
	}
}

func TestReloadDropsOverQuotaAccountWhenAllowOverUsageDisabled(t *testing.T) {
	cfgFile := filepath.Join(t.TempDir(), "config.json")
	if err := config.Init(cfgFile); err != nil {
		t.Fatalf("config.Init: %v", err)
	}
	if err := config.AddAccount(config.Account{
		ID:           "over",
		Enabled:      true,
		UsageCurrent: 10,
		UsageLimit:   10,
	}); err != nil {
		t.Fatalf("AddAccount: %v", err)
	}

	p := newTestPool()
	p.Reload()

	if got := p.GetNext(); got != nil {
		t.Fatalf("expected over-quota account to be dropped, got %q", got.ID)
	}
}
