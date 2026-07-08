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
		cooldowns:        make(map[string]time.Time),
		errorCounts:      make(map[string]int),
		modelLists:       make(map[string]map[string]bool),
		lastUsedAt:       make(map[string]time.Time),
		dynamicIntervals: make(map[string]int64),
	}
	p.accounts = accounts
	return p
}

func TestRecordTTFTBackoffAndRecovery(t *testing.T) {
	p := newTestPool(config.Account{ID: "a"})

	// 慢样本触发首次退避:cur=0 → step。
	p.RecordTTFT("a", ttftTriggerMs+1)
	if got := p.dynamicIntervals["a"]; got != ttftStepMs {
		t.Fatalf("first slow sample: dyn=%d want %d", got, ttftStepMs)
	}
	// 再次慢:加性递增 cur += step。
	p.RecordTTFT("a", ttftTriggerMs+1)
	if got := p.dynamicIntervals["a"]; got != ttftStepMs*2 {
		t.Fatalf("second slow sample: dyn=%d want %d", got, ttftStepMs*2)
	}
	// 中间地带不动。
	prev := p.dynamicIntervals["a"]
	p.RecordTTFT("a", (ttftTriggerMs+ttftRecoverMs)/2)
	if p.dynamicIntervals["a"] != prev {
		t.Fatalf("middle band changed dyn: got %d want %d", p.dynamicIntervals["a"], prev)
	}
	// 快样本:cur/2 直到低于 step 即删除条目。
	p.RecordTTFT("a", ttftRecoverMs-1)
	if p.dynamicIntervals["a"] != prev/2 {
		t.Fatalf("half decay: got %d want %d", p.dynamicIntervals["a"], prev/2)
	}
	p.RecordTTFT("a", ttftRecoverMs-1)
	if _, ok := p.dynamicIntervals["a"]; ok {
		t.Fatalf("expected condition removed when below step, got %d", p.dynamicIntervals["a"])
	}
}

func TestRecordTTFTGuardsZeroAndCap(t *testing.T) {
	p := newTestPool(config.Account{ID: "a"})

	// ttftMs<=0 护栏:不能进入退避、也不会误判为极快。
	p.RecordTTFT("a", 0)
	p.RecordTTFT("a", -1)
	if _, ok := p.dynamicIntervals["a"]; ok {
		t.Fatalf("ttftMs<=0 should not create dyn entry")
	}
	// 持续慢应封顶在 MaxMs(加性 +Step 需要 MaxMs/Step 次到顶,冗余多喂几次)。
	n := int(ttftMaxMs/ttftStepMs) + 5
	for i := 0; i < n; i++ {
		p.RecordTTFT("a", ttftTriggerMs+1)
	}
	if got := p.dynamicIntervals["a"]; got != ttftMaxMs {
		t.Fatalf("cap: dyn=%d want %d", got, ttftMaxMs)
	}
}

func TestDynThrottleSkipsInMainLoopButFallbackStillPicks(t *testing.T) {
	// 回归:上游全局变慢时所有账号同时退避,兜底必须能选出账号(绝不返回 nil)。
	p := newTestPool(
		config.Account{ID: "a"},
		config.Account{ID: "b"},
	)
	// 把两个账号都塞进 dynamic 退避且窗口未过。
	p.lastMu.Lock()
	now := time.Now()
	p.dynamicIntervals["a"] = 60000
	p.dynamicIntervals["b"] = 60000
	p.lastUsedAt["a"] = now
	p.lastUsedAt["b"] = now
	p.lastMu.Unlock()

	acc := p.GetNext()
	if acc == nil {
		t.Fatal("fallback must still pick an account even when all are dyn-throttled, got nil")
	}
}

func TestTryReserveDynSlotIsAtomic(t *testing.T) {
	// 回归:退避窗口刚到期时,连续两次(等价于并发两个 goroutine)只有第一次能抢占,
	// 第二次因为 lastUsedAt 已被第一次原子更新,再看窗口就还未过。
	p := newTestPool(config.Account{ID: "a"})
	p.lastMu.Lock()
	p.dynamicIntervals["a"] = 60000
	p.lastUsedAt["a"] = time.Now().Add(-61 * time.Second) // 窗口刚到期
	p.lastMu.Unlock()

	now := time.Now()
	if !p.tryReserveDynSlot("a", now) {
		t.Fatal("first try after window: expected true, got false")
	}
	if p.tryReserveDynSlot("a", now) {
		t.Fatal("second try in the same instant: expected false (惊群防护), got true")
	}
	// 未退避账号(dyn=0)始终 no-op 放行,不占位。
	if !p.tryReserveDynSlot("nonexistent", now) {
		t.Fatal("unthrottled acc: expected true, got false")
	}
	if !p.tryReserveDynSlot("nonexistent", now) {
		t.Fatal("unthrottled acc second call: still expected true, got false")
	}
}

func TestDynThrottleReleaseAfterWindow(t *testing.T) {
	p := newTestPool(config.Account{ID: "a"})

	// 手工塞一次退避:窗口未到直接被主循环跳过、兜底放行。
	p.lastMu.Lock()
	p.dynamicIntervals["a"] = 60000
	p.lastUsedAt["a"] = time.Now()
	p.lastMu.Unlock()

	// 手动把窗口拨到过去,账号应从主循环正常路径被选中。
	p.lastMu.Lock()
	p.lastUsedAt["a"] = time.Now().Add(-61 * time.Second)
	p.lastMu.Unlock()

	if acc := p.GetNext(); acc == nil || acc.ID != "a" {
		t.Fatalf("pick after window: expected account a, got %#v", acc)
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
