package pool

import (
	"errors"
	"kiro-go/config"
	"path/filepath"
	"testing"
	"time"
)

func newTestPool(accounts ...config.Account) *AccountPool {
	p := &AccountPool{
		cooldowns:      make(map[string]time.Time),
		errorCounts:    make(map[string]int),
		modelLists:     make(map[string]map[string]bool),
		lastUsedAt:     make(map[string]time.Time),
		stickySessions: make(map[string]stickyEntry),
		stickyTTL:      time.Hour,
	}
	p.accounts = accounts
	return p
}

func TestOverLimitAccountsAreSkippedByDefault(t *testing.T) {
	p := newTestPool(
		config.Account{ID: "normal"},
		config.Account{ID: "over", UsageCurrent: 10, UsageLimit: 10},
	)

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
	p := newTestPool(config.Account{
		ID:            "over",
		UsageCurrent:  10,
		UsageLimit:    10,
		OverageStatus: "ENABLED",
	})

	acc := p.GetNext()
	if acc == nil {
		t.Fatalf("expected upstream-enabled overage account to be selectable")
	}
	if acc.ID != "over" {
		t.Fatalf("expected overage account, got %q", acc.ID)
	}
}

func TestOverLimitAccountsRemainSkippedWhenUpstreamOverageDisabled(t *testing.T) {
	p := newTestPool(config.Account{
		ID:            "over",
		UsageCurrent:  10,
		UsageLimit:    10,
		OverageStatus: "DISABLED",
	})

	if acc := p.GetNext(); acc != nil {
		t.Fatalf("expected nil when upstream OverageStatus=DISABLED, got %q", acc.ID)
	}
}

func TestGetNextKeepsFiveMinuteTokenAvailable(t *testing.T) {
	account := config.Account{
		ID:          "acct-1",
		AccessToken: "access-token",
		ExpiresAt:   time.Now().Unix() + 300,
	}
	p := newTestPool(account)

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
	negatives := []string{
		"status code 4011 found",
		"error 14013 exceeded",
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
		"No Available Kiro Profile",
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
	cfgFile := filepath.Join(t.TempDir(), "config.json")
	if err := config.Init(cfgFile); err != nil {
		t.Fatalf("config.Init: %v", err)
	}

	p := newTestPool()
	p.DisableAccount("test-id", "test reason")

	p.mu.Lock()
	cooldown, ok := p.cooldowns["test-id"]
	p.mu.Unlock()

	if !ok {
		t.Fatal("expected cooldown to be set after DisableAccount")
	}
	minExpected := time.Now().Add(23 * time.Hour)
	if cooldown.Before(minExpected) {
		t.Fatalf("expected cooldown >= 23h in future, got %v", cooldown)
	}
}

func TestGetNextExcludingSkipsExcludedAccount(t *testing.T) {
	p := newTestPool(
		config.Account{ID: "a", Enabled: true},
		config.Account{ID: "b", Enabled: true},
	)
	acc := p.GetNextExcluding(map[string]bool{"a": true})
	if acc == nil || acc.ID != "b" {
		t.Fatalf("expected account b, got %#v", acc)
	}
}

func TestGetNextForModelExcludingSkipsExcludedAccount(t *testing.T) {
	p := newTestPool(
		config.Account{ID: "a", Enabled: true},
		config.Account{ID: "b", Enabled: true},
	)
	p.SetModelList("a", []string{"claude-sonnet-4.5"})
	p.SetModelList("b", []string{"claude-sonnet-4.5"})

	acc := p.GetNextForModelExcluding("claude-sonnet-4.5", map[string]bool{"a": true})
	if acc == nil || acc.ID != "b" {
		t.Fatalf("expected account b, got %#v", acc)
	}
}

// ---------------------------------------------------------------------------
// Two-pool LRU scheduling (sensitive-first, LRU within pool)
// ---------------------------------------------------------------------------

// Sensitive accounts (MinIntervalMs > 0) should be picked before normal ones
// when they are within their window budget.
func TestSensitivePoolPreferredOverNormalWhenAvailable(t *testing.T) {
	p := newTestPool(
		config.Account{ID: "normal"},
		config.Account{ID: "sensitive", MinIntervalMs: 20000},
	)
	acc := p.GetNext()
	if acc == nil || acc.ID != "sensitive" {
		t.Fatalf("expected sensitive account to be preferred, got %#v", acc)
	}
}

// After a sensitive account is picked, another request within MinInterval must
// fall back to the normal pool rather than reusing the sensitive one.
func TestSensitiveThrottleFallsBackToNormalPool(t *testing.T) {
	p := newTestPool(
		config.Account{ID: "normal"},
		config.Account{ID: "sensitive", MinIntervalMs: 20000},
	)

	first := p.GetNext()
	if first == nil || first.ID != "sensitive" {
		t.Fatalf("first pick expected sensitive, got %#v", first)
	}
	second := p.GetNext()
	if second == nil || second.ID != "normal" {
		t.Fatalf("second pick within MinInterval expected fallback to normal, got %#v", second)
	}
}

// Two sensitive accounts should be picked round-robin-ish via LRU: whichever
// was used longest ago wins.
func TestSensitivePoolLRUAlternatesBetweenAccounts(t *testing.T) {
	p := newTestPool(
		config.Account{ID: "s1", MinIntervalMs: 20000},
		config.Account{ID: "s2", MinIntervalMs: 20000},
	)

	first := p.GetNext()
	second := p.GetNext()
	if first == nil || second == nil {
		t.Fatalf("expected two picks, got %v / %v", first, second)
	}
	if first.ID == second.ID {
		t.Fatalf("LRU should alternate; got %s twice", first.ID)
	}
}

// When all sensitive accounts are throttled and no normal exists, return nil so
// the handler can respond with 503 (方案 A).
func TestReturnsNilWhenAllSensitiveThrottledAndNoNormal(t *testing.T) {
	p := newTestPool(
		config.Account{ID: "s1", MinIntervalMs: 20000},
	)
	first := p.GetNext()
	if first == nil || first.ID != "s1" {
		t.Fatalf("expected s1 on first pick, got %#v", first)
	}
	second := p.GetNext()
	if second != nil {
		t.Fatalf("expected nil when all sensitive throttled and no normal pool, got %#v", second)
	}
}

// A sensitive account should re-enter the pool once its window elapses.
func TestSensitiveAccountAvailableAgainAfterInterval(t *testing.T) {
	p := newTestPool(
		config.Account{ID: "s1", MinIntervalMs: 50}, // 50ms so the test is fast
	)
	if acc := p.GetNext(); acc == nil || acc.ID != "s1" {
		t.Fatalf("first pick expected s1, got %#v", acc)
	}
	if acc := p.GetNext(); acc != nil {
		t.Fatalf("expected throttle immediately after use, got %#v", acc)
	}
	time.Sleep(70 * time.Millisecond)
	if acc := p.GetNext(); acc == nil || acc.ID != "s1" {
		t.Fatalf("expected s1 to be available again after MinInterval, got %#v", acc)
	}
}

// LRU among normal accounts: after picking A, the next pick should be B, not A.
func TestNormalPoolLRUAlternates(t *testing.T) {
	p := newTestPool(
		config.Account{ID: "a"},
		config.Account{ID: "b"},
	)
	first := p.GetNext()
	second := p.GetNext()
	if first == nil || second == nil || first.ID == second.ID {
		t.Fatalf("expected LRU alternation, got %v / %v", first, second)
	}
}

// ---------------------------------------------------------------------------
// RecordError cooldown durations
// ---------------------------------------------------------------------------

func TestRecordErrorQuotaUsesShortCooldown(t *testing.T) {
	p := newTestPool(config.Account{ID: "x"})
	p.RecordError("x", true)

	p.mu.Lock()
	cd, ok := p.cooldowns["x"]
	p.mu.Unlock()
	if !ok {
		t.Fatal("expected quota error to set a cooldown")
	}
	// Must be short: within a few seconds of quotaCooldown (60s) — definitely not 1h.
	if cd.After(time.Now().Add(5 * time.Minute)) {
		t.Fatalf("expected quota cooldown < 5min, got %v", time.Until(cd))
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

// ---------------------------------------------------------------------------
// Sticky session routing (仅正常池)
// ---------------------------------------------------------------------------

func newStickyTestPool(accounts ...config.Account) *AccountPool {
	p := newTestPool(accounts...)
	p.stickyTTL = time.Hour
	return p
}

// 会话上次成功用的是正常池账号 b，同一会话再来应仍复用 b（即使 a 也可用）。
func TestGetForSessionReusesLastNormalAccount(t *testing.T) {
	p := newStickyTestPool(
		config.Account{ID: "a"},
		config.Account{ID: "b"},
	)
	p.RecordStickySuccess("conv-1", "b")

	for i := 0; i < 5; i++ {
		acc := p.GetForSession("conv-1", "", nil)
		if acc == nil || acc.ID != "b" {
			t.Fatalf("iter %d: expected sticky b, got %#v", i, acc)
		}
	}
}

// 敏感池账号成功不应该被写进 sticky 表——它们跟粘性冲突。
func TestRecordStickySuccessSkipsSensitiveAccounts(t *testing.T) {
	p := newStickyTestPool(
		config.Account{ID: "s", MinIntervalMs: 60000},
		config.Account{ID: "n"},
	)
	p.RecordStickySuccess("conv-1", "s")

	p.mu.Lock()
	_, tracked := p.stickySessions["conv-1"]
	p.mu.Unlock()
	if tracked {
		t.Fatal("expected sensitive account to not be recorded in sticky map")
	}
}

// 敏感池账号可用时应优先(两池 LRU)，sticky 里指着正常池账号不干扰这个优先级。
func TestGetForSessionStillPrefersSensitivePool(t *testing.T) {
	p := newStickyTestPool(
		config.Account{ID: "sensitive", MinIntervalMs: 60000},
		config.Account{ID: "normal"},
	)
	p.RecordStickySuccess("conv-1", "normal")

	acc := p.GetForSession("conv-1", "", nil)
	if acc == nil || acc.ID != "sensitive" {
		t.Fatalf("sensitive should still be preferred, got %#v", acc)
	}
}

// 敏感池被节流时，落到正常池路径，此时应命中 sticky。
func TestGetForSessionUsesStickyOnSensitiveThrottle(t *testing.T) {
	p := newStickyTestPool(
		config.Account{ID: "sensitive", MinIntervalMs: 60000},
		config.Account{ID: "n1"},
		config.Account{ID: "n2"},
	)
	// 先耗掉敏感池。
	if acc := p.GetForSession("conv-1", "", nil); acc == nil || acc.ID != "sensitive" {
		t.Fatalf("first pick should be sensitive, got %#v", acc)
	}
	// 之前 sticky 指向 n2；节流下应复用 n2 而不是 LRU 顺位 n1。
	p.RecordStickySuccess("conv-1", "n2")
	acc := p.GetForSession("conv-1", "", nil)
	if acc == nil || acc.ID != "n2" {
		t.Fatalf("expected sticky n2 when sensitive throttled, got %#v", acc)
	}
}

// sticky 指向的账号被 excluded 时，回退到正常池 LRU。
func TestGetForSessionFallsBackWhenStickyAccountExcluded(t *testing.T) {
	p := newStickyTestPool(
		config.Account{ID: "a"},
		config.Account{ID: "b"},
	)
	p.RecordStickySuccess("conv-1", "a")

	acc := p.GetForSession("conv-1", "", map[string]bool{"a": true})
	if acc == nil || acc.ID != "b" {
		t.Fatalf("expected fallback to b when sticky excluded, got %#v", acc)
	}
}

// sticky 指向的账号在 cooldown 时，回退到正常池 LRU。
func TestGetForSessionFallsBackWhenStickyAccountInCooldown(t *testing.T) {
	p := newStickyTestPool(
		config.Account{ID: "a"},
		config.Account{ID: "b"},
	)
	p.RecordStickySuccess("conv-1", "a")
	p.mu.Lock()
	p.cooldowns["a"] = time.Now().Add(time.Minute)
	p.mu.Unlock()

	acc := p.GetForSession("conv-1", "", nil)
	if acc == nil || acc.ID != "b" {
		t.Fatalf("expected fallback to b when sticky in cooldown, got %#v", acc)
	}
}

// sticky 条目过期后应被 prune，一次 GetForSession 后从表中删除。
func TestGetForSessionExpiredStickyIsDropped(t *testing.T) {
	p := newStickyTestPool(
		config.Account{ID: "a"},
		config.Account{ID: "b"},
	)
	p.mu.Lock()
	p.stickySessions["conv-1"] = stickyEntry{
		AccountID: "a",
		ExpiresAt: time.Now().Add(-time.Minute),
	}
	p.mu.Unlock()

	_ = p.GetForSession("conv-1", "", nil)

	p.mu.Lock()
	_, present := p.stickySessions["conv-1"]
	p.mu.Unlock()
	if present {
		t.Fatal("expected expired sticky entry to be dropped")
	}
}

// sessionKey 为空时应完全退化为 LRU。
func TestGetForSessionIgnoresEmptySessionKey(t *testing.T) {
	p := newStickyTestPool(config.Account{ID: "a"})
	acc := p.GetForSession("", "", nil)
	if acc == nil || acc.ID != "a" {
		t.Fatalf("expected round robin when sessionKey empty, got %#v", acc)
	}
}

// sticky 开关关掉后，RecordStickySuccess 不落表，GetForSession 也退化为 LRU。
func TestStickyDisabledSwitch(t *testing.T) {
	cfgFile := filepath.Join(t.TempDir(), "config.json")
	if err := config.Init(cfgFile); err != nil {
		t.Fatalf("config.Init: %v", err)
	}
	if err := config.UpdateStickySessionRouting(false); err != nil {
		t.Fatalf("UpdateStickySessionRouting: %v", err)
	}
	t.Cleanup(func() { _ = config.UpdateStickySessionRouting(true) })

	p := newStickyTestPool(
		config.Account{ID: "a"},
		config.Account{ID: "b"},
	)
	p.RecordStickySuccess("conv-1", "b")

	p.mu.Lock()
	_, tracked := p.stickySessions["conv-1"]
	p.mu.Unlock()
	if tracked {
		t.Fatal("expected no sticky record when switch is off")
	}

	if acc := p.GetForSession("conv-1", "", nil); acc == nil {
		t.Fatal("expected LRU fallback account, got nil")
	}
}

// Reload 时若 sticky 里的账号变成敏感账号(MinIntervalMs 被后台改大)，条目应被清掉。
func TestReloadPurgesStickyPointingToSensitive(t *testing.T) {
	cfgFile := filepath.Join(t.TempDir(), "config.json")
	if err := config.Init(cfgFile); err != nil {
		t.Fatalf("config.Init: %v", err)
	}
	if err := config.AddAccount(config.Account{ID: "x", Enabled: true}); err != nil {
		t.Fatalf("AddAccount: %v", err)
	}

	p := newStickyTestPool()
	p.stickyTTL = time.Hour
	p.Reload()
	p.RecordStickySuccess("conv-1", "x")

	// 现在把 x 改成敏感账号，再 Reload。
	if err := config.UpdateAccount("x", config.Account{ID: "x", Enabled: true, MinIntervalMs: 60000}); err != nil {
		t.Fatalf("UpdateAccount: %v", err)
	}
	p.Reload()

	p.mu.Lock()
	_, present := p.stickySessions["conv-1"]
	p.mu.Unlock()
	if present {
		t.Fatal("expected sticky pointing to now-sensitive account to be pruned on Reload")
	}
}

// StickySessionStats 只返回聚合计数，不泄漏 sessionKey。
func TestStickySessionStatsCountsActiveOnly(t *testing.T) {
	p := newStickyTestPool()
	now := time.Now()
	p.mu.Lock()
	p.stickySessions["active"] = stickyEntry{AccountID: "a", ExpiresAt: now.Add(time.Minute)}
	p.stickySessions["expired"] = stickyEntry{AccountID: "b", ExpiresAt: now.Add(-time.Minute)}
	p.mu.Unlock()

	stats := p.StickySessionStats()
	if got, want := stats["activeSessions"].(int), 1; got != want {
		t.Fatalf("activeSessions = %d, want %d", got, want)
	}
	perAccount, ok := stats["sessionsByAccount"].(map[string]int)
	if !ok {
		t.Fatalf("sessionsByAccount type = %T, want map[string]int", stats["sessionsByAccount"])
	}
	if perAccount["a"] != 1 {
		t.Fatalf("expected account a to have 1 active session, got %d", perAccount["a"])
	}
	if _, present := perAccount["b"]; present {
		t.Fatal("expected expired sticky to be excluded from sessionsByAccount")
	}
}
