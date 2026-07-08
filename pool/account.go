// Package pool 账号池管理
// 实现轮询负载均衡、错误冷却、Token 刷新
package pool

import (
	"kiro-go/config"
	"kiro-go/logger"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const tokenRefreshSkewSeconds int64 = 120

// TTFT 动态退避（AI-MD：加性退避 + 乘性衰减）常量。账号首字节耗时变慢时自动
// 拉大最小调用间隔，变快后乘性衰减恢复，无需手工配置。调参改这里即可。
const (
	ttftTriggerMs int64 = 15000   // EWMA 超过此值 → 加性退避(每次 +Step)
	ttftRecoverMs int64 = 10000   // 当次原始 TTFT 低于此值 → 乘性衰减恢复(低于 Trigger 构成迟滞防抖)
	ttftStepMs    int64 = 60000   // 退避起步值 & 每次增量 & 归零阈值 (1min)
	ttftMaxMs     int64 = 3600000 // 退避封顶 60min(事实熔断上限,仍每 60min 自然探测一次)

	// ttftEwmaAlpha 是新样本在 EWMA 里的权重,历史权重为 1-alpha。仅用于加性退避判定,
	// 目的是防止单次偶发慢请求(如一次大 prompt)在有历史基线时误触发;冷启动(无历史)
	// 时首个样本直接作为 EWMA 初值,不做稀释,依旧能被单次极慢样本触发。
	ttftEwmaAlpha float64 = 0.3
)

// quotaCooldown 是收到 429 后的账号冷却时长。不需要一次 429 就把账号打入 1h 冷宫,
// 但也不能太短,避免短冷却后立刻撞回同一个限流账号。
const quotaCooldown = 5 * time.Minute

// stickyEntry 记录某个会话上次成功使用的账号，用于提升 prompt cache 命中率。
type stickyEntry struct {
	AccountID string
	ExpiresAt time.Time
}

// AccountPool 账号池
type AccountPool struct {
	mu            sync.RWMutex
	accounts      []config.Account
	totalAccounts int
	currentIndex  uint64
	cooldowns     map[string]time.Time       // 账号冷却时间
	errorCounts   map[string]int             // 连续错误计数
	modelLists    map[string]map[string]bool // accountID → set of modelIDs (from ListAvailableModels)

	stickySessions map[string]stickyEntry // conversationID → 上次成功服务它的账号
	stickyTTL      time.Duration          // 会话粘性存活时间

	lastMu           sync.Mutex           // 保护 lastUsedAt / dynamicIntervals / ttftEwma(选号路径持 RLock,需独立锁做写)
	lastUsedAt       map[string]time.Time // accountID → 上次被选中的时间(TTFT 退避节流用)
	dynamicIntervals map[string]int64     // accountID → 当前 TTFT 退避间隔(ms)，0/缺失表示未退避
	ttftEwma         map[string]float64   // accountID → TTFT 的指数滑动平均(ms)，仅用于加性退避判定，缺失表示尚无样本
}

var (
	pool     *AccountPool
	poolOnce sync.Once
)

// GetPool 获取全局账号池单例
func GetPool() *AccountPool {
	poolOnce.Do(func() {
		pool = &AccountPool{
			cooldowns:        make(map[string]time.Time),
			errorCounts:      make(map[string]int),
			modelLists:       make(map[string]map[string]bool),
			lastUsedAt:       make(map[string]time.Time),
			dynamicIntervals: make(map[string]int64),
			ttftEwma:         make(map[string]float64),
			stickySessions:   make(map[string]stickyEntry),
			stickyTTL:        config.GetStickySessionTTL(),
		}
		pool.Reload()
	})
	return pool
}

// Reload rebuilds the weighted account list from config.
// Weight <= 1 → 1 entry; weight >= 2 → weight entries.
// Over-quota accounts are dropped unless either the per-account upstream
// Overages switch (OverageStatus=ENABLED) or the global AllowOverUsage
// setting permits over-quota routing.
func (p *AccountPool) Reload() {
	p.mu.Lock()
	defer p.mu.Unlock()
	enabled := config.GetEnabledAccounts()
	allowOverUsage := config.GetAllowOverUsage()
	var weighted []config.Account
	for _, a := range enabled {
		if isQuotaBlocked(a, allowOverUsage) {
			continue
		}
		w := effectiveWeight(a.Weight)
		for j := 0; j < w; j++ {
			weighted = append(weighted, a)
		}
	}
	p.accounts = weighted
	p.totalAccounts = len(enabled)
	p.stickyTTL = config.GetStickySessionTTL()
	p.pruneExpiredStickyLocked(time.Now())
}

// tryReserveDynSlot 原子地"判是否处于 TTFT 退避窗口 + 通过则占位"。
// 返回 true 表示放行(未退避,或退避但窗口已过并已抢占),false 表示节流中(应跳过)。
// 把"判定→占位"合并进一次 lastMu 内,消除退避窗口到期瞬间多个并发请求同时命中
// 同一账号的惊群窗口。仅供选号主循环 / sticky 命中路径调用;兜底路径继续用 markPicked。
func (p *AccountPool) tryReserveDynSlot(accID string, now time.Time) bool {
	p.lastMu.Lock()
	defer p.lastMu.Unlock()
	dyn := p.dynamicIntervals[accID]
	if dyn <= 0 {
		return true
	}
	if now.Sub(p.lastUsedAt[accID]) < time.Duration(dyn)*time.Millisecond {
		return false
	}
	p.lastUsedAt[accID] = now
	return true
}

// markPicked 记录账号被选中的时间(仅兜底路径使用)。
// 主循环 / sticky 命中路径已改用 tryReserveDynSlot 一次性完成判定与占位;
// 兜底忽略退避直接命中,仍需要在此写 lastUsedAt 以防连续兜底反复命中同一账号。
func (p *AccountPool) markPicked(acc *config.Account, now time.Time) {
	if acc == nil {
		return
	}
	p.lastMu.Lock()
	defer p.lastMu.Unlock()
	if p.dynamicIntervals[acc.ID] <= 0 {
		return
	}
	p.lastUsedAt[acc.ID] = now
}

// RecordTTFT 用一次成功请求的真实首字节耗时驱动该账号的自适应退避间隔(AIMD)。
// 加性退避看 EWMA(平滑掉单次偶发慢请求的误触发),乘性衰减看当次原始值(避免退避期间
// 请求变稀疏后 EWMA 迟迟追不上真实好转,把恢复卡死)。中间地带迟滞不动。
// 只在成功拿到首字节的路径调用(recordSuccessLog)。
func (p *AccountPool) RecordTTFT(accountID string, ttftMs int64) {
	// ttftMs 仅在 OnFirstToken 回调赋值,成功但无首字节回调时为 0,拦掉避免误判为极快。
	if ttftMs <= 0 {
		return
	}
	p.lastMu.Lock()
	defer p.lastMu.Unlock()

	ewma, seeded := p.ttftEwma[accountID]
	if !seeded {
		ewma = float64(ttftMs) // 冷启动无历史可稀释,首个样本即 EWMA 初值
	} else {
		ewma = ewma*(1-ttftEwmaAlpha) + float64(ttftMs)*ttftEwmaAlpha
	}
	p.ttftEwma[accountID] = ewma

	cur := p.dynamicIntervals[accountID]
	switch {
	case ttftMs < ttftRecoverMs:
		// 恢复用当次原始值,不看 EWMA:退避期间请求间隔被拉长、样本变稀疏,
		// 若也靠 EWMA 判断会导致均值迟迟降不下来,账号明明已恢复却持续被加深退避。
		if cur == 0 {
			return
		}
		next := cur / 2
		if next < ttftStepMs {
			delete(p.dynamicIntervals, accountID)
			logger.Infof("[TTFTThrottle] 恢复正常 account=%s ttft=%dms ewma=%.0fms", accountID, ttftMs, ewma)
		} else {
			p.dynamicIntervals[accountID] = next
		}
	case ewma > float64(ttftTriggerMs):
		next := cur + ttftStepMs
		if next > ttftMaxMs {
			next = ttftMaxMs
		}
		p.dynamicIntervals[accountID] = next
		if cur == 0 {
			logger.Warnf("[TTFTThrottle] 进入退避 account=%s ttft=%dms ewma=%.0fms interval=%dms", accountID, ttftMs, ewma, next)
		}
	}
}

// TTFTStatus 是某账号 TTFT 自适应退避状态的只读快照,供 API/前端展示用。
type TTFTStatus struct {
	EwmaMs             float64 // 当前 TTFT 指数滑动平均(ms),仅用于加性退避判定
	BackoffMs          int64   // 当前 AIMD 退避间隔(ms),0 表示未退避
	BackoffRemainingMs int64   // 距离下一次可被选中还剩多少 ms(窗口已过则为 0,但仍处于退避状态)
}

// GetCooldowns 返回当前仍处于冷却中的账号快照(accountID → 冷却截止时间的 Unix 秒)。
// 冷却来源包括 429/连续错误(RecordError)、禁用兜底(DisableAccount)等写入点,
// 统一供账号列表展示"冷却中"状态用;已过期的条目不返回。
func (p *AccountPool) GetCooldowns() map[string]int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	now := time.Now()
	result := make(map[string]int64, len(p.cooldowns))
	for id, until := range p.cooldowns {
		if now.Before(until) {
			result[id] = until.Unix()
		}
	}
	return result
}

// GetTTFTStatuses 返回所有已产生过成功 TTFT 样本的账号快照(accountID → EWMA/退避状态)。
// 只要 RecordTTFT 记录过至少一次样本就会出现在结果里,不要求正处于退避中;
// 供账号列表/详情页展示 TTFT 均值与退避状态用。
func (p *AccountPool) GetTTFTStatuses() map[string]TTFTStatus {
	p.lastMu.Lock()
	defer p.lastMu.Unlock()
	now := time.Now()
	result := make(map[string]TTFTStatus, len(p.ttftEwma))
	for id, ewma := range p.ttftEwma {
		st := TTFTStatus{EwmaMs: ewma}
		if interval := p.dynamicIntervals[id]; interval > 0 {
			remaining := interval - now.Sub(p.lastUsedAt[id]).Milliseconds()
			if remaining < 0 {
				remaining = 0
			}
			st.BackoffMs = interval
			st.BackoffRemainingMs = remaining
		}
		result[id] = st
	}
	return result
}

// GetNext 获取下一个可用账号（加权轮询）
func (p *AccountPool) GetNext() *config.Account {
	return p.GetNextExcluding(nil)
}

// GetNextExcluding 获取下一个可用账号（加权轮询），并跳过指定账号。
func (p *AccountPool) GetNextExcluding(excluded map[string]bool) *config.Account {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.accounts) == 0 {
		return nil
	}

	allowOverUsage := config.GetAllowOverUsage()
	now := time.Now()
	n := len(p.accounts)
	seen := make(map[string]bool)

	// 加权轮询查找可用账号
	for i := 0; i < n; i++ {
		idx := atomic.AddUint64(&p.currentIndex, 1) % uint64(n)
		acc := &p.accounts[idx]

		if excluded != nil && excluded[acc.ID] {
			seen[acc.ID] = true
			continue
		}
		if seen[acc.ID] {
			continue
		}

		// 跳过冷却中的账号
		if cooldown, ok := p.cooldowns[acc.ID]; ok && now.Before(cooldown) {
			seen[acc.ID] = true
			continue
		}

		// 跳过即将过期的 Token
		if acc.ExpiresAt > 0 && time.Now().Unix() > acc.ExpiresAt-tokenRefreshSkewSeconds {
			seen[acc.ID] = true
			continue
		}

		// Skip accounts whose quota is exhausted, unless overrides apply.
		if isQuotaBlocked(*acc, allowOverUsage) {
			seen[acc.ID] = true
			continue
		}

		// TTFT 退避 CAS: 原子完成"判定是否处于退避窗口 + 通过则占位",
		// 消除退避到期瞬间多个并发请求同时命中同一账号的惊群。
		if !p.tryReserveDynSlot(acc.ID, now) {
			seen[acc.ID] = true
			continue
		}
		return acc
	}

	// 无可用账号(含 TTFT 退避账号,兜底故意忽略退避),交给 fallbackPick 挑一个顶上。
	return p.fallbackPick(excluded, allowOverUsage, now, nil)
}

// fallbackPick 兜底选号：主循环转了一整圈都没选出账号时用(可能全员在 TTFT 退避、
// 或 429/连续错误冷却)。兜底故意无视 TTFT 退避——上游整体变慢时几乎所有账号会
// 同时触发退避,若兜底也遵守退避会导致整池返回 nil,代价比"明知偏慢也硬发"大得多。
// 优先级:
//  1. 没有 429/连续错误冷却的候选里,选 TTFT EWMA 最小的(退避最轻/从未观测到慢的账号
//     天然 EWMA 缺失→视为 0,优先命中);
//  2. 全员都有 429/连续错误冷却时,退化为选冷却最快到期的那个。
//
// extraFilter 用于 GetNextForModelExcluding 额外按模型筛选,GetNextExcluding 传 nil。
func (p *AccountPool) fallbackPick(excluded map[string]bool, allowOverUsage bool, now time.Time, extraFilter func(acc *config.Account) bool) *config.Account {
	p.lastMu.Lock()
	ewmaSnapshot := make(map[string]float64, len(p.ttftEwma))
	for id, v := range p.ttftEwma {
		ewmaSnapshot[id] = v
	}
	p.lastMu.Unlock()

	var cooldownBest *config.Account
	var earliest time.Time
	var noCooldownBest *config.Account
	bestEwma := math.MaxFloat64
	for i := range p.accounts {
		acc := &p.accounts[i]
		if excluded != nil && excluded[acc.ID] {
			continue
		}
		if extraFilter != nil && !extraFilter(acc) {
			continue
		}
		if isQuotaBlocked(*acc, allowOverUsage) {
			continue
		}
		if cooldown, ok := p.cooldowns[acc.ID]; ok {
			if cooldownBest == nil || cooldown.Before(earliest) {
				cooldownBest = acc
				earliest = cooldown
			}
			continue
		}
		if ewma := ewmaSnapshot[acc.ID]; noCooldownBest == nil || ewma < bestEwma {
			noCooldownBest = acc
			bestEwma = ewma
		}
	}
	if noCooldownBest != nil {
		p.markPicked(noCooldownBest, now)
		return noCooldownBest
	}
	p.markPicked(cooldownBest, now)
	return cooldownBest
}

// SetModelList 缓存账号支持的模型集合（由 handler 在刷新后调用）
func (p *AccountPool) SetModelList(accountID string, modelIDs []string) {
	set := make(map[string]bool, len(modelIDs))
	for _, id := range modelIDs {
		set[strings.ToLower(strings.TrimSpace(id))] = true
	}
	p.mu.Lock()
	p.modelLists[accountID] = set
	p.mu.Unlock()
}

// GetModelList 返回该账号缓存的模型 ID 列表（供 admin API 使用）。
// 若尚无缓存则返回空切片。
func (p *AccountPool) GetModelList(accountID string) []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	set, ok := p.modelLists[accountID]
	if !ok || len(set) == 0 {
		return []string{}
	}
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	return ids
}

// accountHasModel 检查账号是否支持指定模型。
// 若该账号尚无模型列表（冷启动），视为支持所有模型。
func (p *AccountPool) accountHasModel(accountID, model string) bool {
	list, ok := p.modelLists[accountID]
	if !ok || len(list) == 0 {
		return true // 冷启动：列表未就绪，乐观放行
	}
	return list[strings.ToLower(strings.TrimSpace(model))]
}

// GetNextForModel 获取下一个支持指定模型的可用账号。
// model 应为去掉 thinking 后缀的实际模型名。
// 若无账号有该模型列表数据，行为与 GetNext 相同（乐观路由）。
func (p *AccountPool) GetNextForModel(model string) *config.Account {
	return p.GetNextForModelExcluding(model, nil)
}

// GetNextForModelExcluding 获取下一个支持指定模型的可用账号，并跳过指定账号。
func (p *AccountPool) GetNextForModelExcluding(model string, excluded map[string]bool) *config.Account {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.accounts) == 0 {
		return nil
	}

	allowOverUsage := config.GetAllowOverUsage()
	now := time.Now()
	n := len(p.accounts)
	seen := make(map[string]bool)

	for i := 0; i < n; i++ {
		idx := atomic.AddUint64(&p.currentIndex, 1) % uint64(n)
		acc := &p.accounts[idx]

		if excluded != nil && excluded[acc.ID] {
			seen[acc.ID] = true
			continue
		}
		if seen[acc.ID] {
			continue
		}
		if !p.accountHasModel(acc.ID, model) {
			seen[acc.ID] = true
			continue
		}
		if cooldown, ok := p.cooldowns[acc.ID]; ok && now.Before(cooldown) {
			seen[acc.ID] = true
			continue
		}
		if acc.ExpiresAt > 0 && time.Now().Unix() > acc.ExpiresAt-tokenRefreshSkewSeconds {
			seen[acc.ID] = true
			continue
		}
		if isQuotaBlocked(*acc, allowOverUsage) {
			seen[acc.ID] = true
			continue
		}
		// TTFT 退避 CAS: 消除惊群。见 GetNextExcluding 同名调用点注释。
		if !p.tryReserveDynSlot(acc.ID, now) {
			seen[acc.ID] = true
			continue
		}
		return acc
	}

	// 无可用账号(含 TTFT 退避账号)，交给 fallbackPick 挑一个顶上，见其注释。
	return p.fallbackPick(excluded, allowOverUsage, now, func(acc *config.Account) bool {
		return p.accountHasModel(acc.ID, model)
	})
}

// GetForSession 会话粘性路由：优先复用 sessionKey（对话 ID）上次成功服务过它的账号，
// 以提升该账号上模拟 prompt cache 的命中率；粘性账号不存在/已过期/不可用时，
// 无缝回退到 GetNextForModelExcluding 的加权轮询逻辑。
func (p *AccountPool) GetForSession(sessionKey, model string, excluded map[string]bool) *config.Account {
	if sessionKey == "" || !config.GetStickySessionRouting() {
		return p.GetNextForModelExcluding(model, excluded)
	}

	p.mu.RLock()
	var candidate *config.Account
	if entry, ok := p.stickySessions[sessionKey]; ok && time.Now().Before(entry.ExpiresAt) {
		allowOverUsage := config.GetAllowOverUsage()
		now := time.Now()
		for i := range p.accounts {
			acc := &p.accounts[i]
			if acc.ID != entry.AccountID {
				continue
			}
			if excluded != nil && excluded[acc.ID] {
				break
			}
			if !p.accountHasModel(acc.ID, model) {
				break
			}
			if cooldown, ok := p.cooldowns[acc.ID]; ok && now.Before(cooldown) {
				break
			}
			if acc.ExpiresAt > 0 && now.Unix() > acc.ExpiresAt-tokenRefreshSkewSeconds {
				break
			}
			if isQuotaBlocked(*acc, allowOverUsage) {
				break
			}
			// TTFT 退避 CAS: 消除惊群。sticky 命中同一个 session 通常无并发,
			// 但多个不同 session 命中同一账号时仍可能撞车,统一走 CAS 更稳。
			if !p.tryReserveDynSlot(acc.ID, now) {
				break
			}
			found := *acc
			candidate = &found
			break
		}
	}
	p.mu.RUnlock()

	if candidate != nil {
		return candidate
	}
	return p.GetNextForModelExcluding(model, excluded)
}

// RecordStickySuccess 在请求成功后写入/刷新会话粘性映射。
// 只在成功路径调用（而非选中账号时），这样一次失败不会把会话永久锁定在坏账号上：
// 失败时不写入，excluded 会在本次请求内排除掉它，下次重试自然换到别的账号。
func (p *AccountPool) RecordStickySuccess(sessionKey, accountID string) {
	if sessionKey == "" || accountID == "" || !config.GetStickySessionRouting() {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	// 退避账号即使写了粘性,GetForSession 命中时 isDynThrottled 会自动回退轮询,无 bug。
	now := time.Now()
	p.pruneExpiredStickyLocked(now)
	p.stickySessions[sessionKey] = stickyEntry{
		AccountID: accountID,
		ExpiresAt: now.Add(p.stickyTTL),
	}
}

// pruneExpiredStickyLocked 清理过期的会话粘性条目。调用方需持有 p.mu 写锁。
func (p *AccountPool) pruneExpiredStickyLocked(now time.Time) {
	for key, entry := range p.stickySessions {
		if !entry.ExpiresAt.After(now) {
			delete(p.stickySessions, key)
		}
	}
}

// StickySessionStats 返回会话粘性表的聚合统计，供 admin 状态接口展示。
// 不暴露明文 sessionKey -> accountID 映射，只给计数信息。
func (p *AccountPool) StickySessionStats() map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()
	now := time.Now()
	active := 0
	perAccount := make(map[string]int)
	for _, entry := range p.stickySessions {
		if now.Before(entry.ExpiresAt) {
			active++
			perAccount[entry.AccountID]++
		}
	}
	return map[string]interface{}{
		"activeSessions":    active,
		"totalTracked":      len(p.stickySessions),
		"sessionsByAccount": perAccount,
	}
}

// GetByID 根据 ID 获取账号
func (p *AccountPool) GetByID(id string) *config.Account {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for i := range p.accounts {
		if p.accounts[i].ID == id {
			return &p.accounts[i]
		}
	}
	return nil
}

// RecordSuccess 记录请求成功，清除冷却
func (p *AccountPool) RecordSuccess(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.cooldowns, id)
	p.errorCounts[id] = 0
}

// RecordError 记录请求错误，设置冷却
func (p *AccountPool) RecordError(id string, isQuotaError bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.errorCounts[id]++

	if isQuotaError {
		// 429：冷却 5 分钟，避免账号在窗口内反复撞限流。
		p.cooldowns[id] = time.Now().Add(quotaCooldown)
	} else if p.errorCounts[id] >= 3 {
		// 连续 3 次错误，冷却 1 分钟
		p.cooldowns[id] = time.Now().Add(time.Minute)
	}
}

// IsAuthFailure reports whether an error indicates the refresh token / credentials
// have been revoked or invalidated upstream (401, 403 with auth markers, etc.).
// These accounts cannot be recovered automatically and must be re-authenticated.
func IsAuthFailure(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	lower := strings.ToLower(msg)

	// Match HTTP status codes only when they appear as standalone tokens to avoid
	// false positives from arbitrary digits in the error body (e.g. request IDs).
	if hasStatusToken(msg, "401") || hasStatusToken(msg, "403") {
		return true
	}
	if strings.Contains(lower, "bad credentials") ||
		strings.Contains(lower, "invalid_grant") ||
		strings.Contains(lower, "invalid grant") ||
		strings.Contains(lower, "invalid_token") ||
		strings.Contains(lower, "invalid token") ||
		strings.Contains(lower, "token expired") ||
		strings.Contains(lower, "token has expired") ||
		strings.Contains(lower, "unauthorized") {
		return true
	}
	return false
}

// hasStatusToken returns true when status appears in s with non-digit boundaries
// on both sides, so "401" matches "HTTP 401 from ..." but not "request_401abc".
func hasStatusToken(s, status string) bool {
	for {
		idx := strings.Index(s, status)
		if idx < 0 {
			return false
		}
		leftOK := idx == 0 || !isDigit(s[idx-1])
		rightIdx := idx + len(status)
		rightOK := rightIdx >= len(s) || !isDigit(s[rightIdx])
		if leftOK && rightOK {
			return true
		}
		s = s[idx+len(status):]
	}
}

func isDigit(b byte) bool {
	return b >= '0' && b <= '9'
}

// IsSuspensionError reports whether the error indicates the account has been
// temporarily suspended by upstream or has no available Kiro profile.
// Unlike auth failures (revoked credentials), these may be transient, but
// the account should be disabled until an operator re-enables it.
func IsSuspensionError(err error) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "temporarily_suspended") ||
		strings.Contains(lower, "temporarily suspended") ||
		strings.Contains(lower, "no available kiro profile")
}

// DisableAccount marks an account as disabled (auth revoked / unrecoverable),
// removes it from the in-memory pool so subsequent requests skip it, and
// persists the change via config.SetAccountBanStatus.
func (p *AccountPool) DisableAccount(id, reason string) {
	if err := config.SetAccountBanStatus(id, "DISABLED", reason); err != nil {
		// best effort — even if persistence fails, drop it from memory
		_ = err
	}
	p.mu.Lock()
	// Long cooldown as a safety net in case Reload races
	p.cooldowns[id] = time.Now().Add(24 * time.Hour)
	p.mu.Unlock()
	p.Reload()
}

// MarkOverLimit marks an account as over usage limit (after a 402 / OVERAGE response).
// With the upstream OverageStatus model, the live status is refreshed via
// FetchOverageStatus from the request handler; here we just cooldown briefly so
// the next attempt picks a different account, then reload.
func (p *AccountPool) MarkOverLimit(id string) {
	p.mu.Lock()
	p.cooldowns[id] = time.Now().Add(time.Hour)
	p.mu.Unlock()
	p.Reload()
}

// UpdateToken 更新账号 Token
func (p *AccountPool) UpdateToken(id, accessToken, refreshToken string, expiresAt int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.accounts {
		if p.accounts[i].ID == id {
			p.accounts[i].AccessToken = accessToken
			if refreshToken != "" {
				p.accounts[i].RefreshToken = refreshToken
			}
			p.accounts[i].ExpiresAt = expiresAt
		}
	}
}

// Count 返回账号总数
func (p *AccountPool) Count() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.totalAccounts > 0 {
		return p.totalAccounts
	}

	seen := make(map[string]bool)
	for _, acc := range p.accounts {
		seen[acc.ID] = true
	}
	return len(seen)
}

// AvailableCount 返回可用账号数
func (p *AccountPool) AvailableCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	now := time.Now()
	count := 0
	seen := make(map[string]bool)
	for _, acc := range p.accounts {
		if seen[acc.ID] {
			continue
		}
		seen[acc.ID] = true
		if cooldown, ok := p.cooldowns[acc.ID]; ok && now.Before(cooldown) {
			continue
		}
		count++
	}
	return count
}

// UpdateStats 更新账号统计
func (p *AccountPool) UpdateStats(id string, tokens int, credits float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var updated bool
	var requestCount, errorCount, totalTokens int
	var totalCredits float64
	var lastUsed int64
	for i := range p.accounts {
		if p.accounts[i].ID == id {
			if !updated {
				p.accounts[i].RequestCount++
				p.accounts[i].TotalTokens += tokens
				p.accounts[i].TotalCredits += credits
				p.accounts[i].LastUsed = time.Now().Unix()

				requestCount = p.accounts[i].RequestCount
				errorCount = p.accounts[i].ErrorCount
				totalTokens = p.accounts[i].TotalTokens
				totalCredits = p.accounts[i].TotalCredits
				lastUsed = p.accounts[i].LastUsed
				updated = true
				continue
			}
			p.accounts[i].RequestCount = requestCount
			p.accounts[i].ErrorCount = errorCount
			p.accounts[i].TotalTokens = totalTokens
			p.accounts[i].TotalCredits = totalCredits
			p.accounts[i].LastUsed = lastUsed
		}
	}
	if updated {
		go config.UpdateAccountStats(id, requestCount, errorCount, totalTokens, totalCredits, lastUsed)
	}
}

// GetAllAccounts 获取所有账号副本
func (p *AccountPool) GetAllAccounts() []config.Account {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make([]config.Account, len(p.accounts))
	copy(result, p.accounts)
	return result
}

func isOverUsageLimit(acc config.Account) bool {
	return acc.UsageLimit > 0 && acc.UsageCurrent >= acc.UsageLimit
}

// isQuotaBlocked reports whether an over-quota account should be skipped:
// the per-account upstream Overages switch (OverageStatus=ENABLED) and the
// global allowOverUsage setting are the two ways to keep it routable.
func isQuotaBlocked(acc config.Account, allowOverUsage bool) bool {
	return isOverUsageLimit(acc) && !isUpstreamOverageEnabled(acc) && !allowOverUsage
}

// isUpstreamOverageEnabled reports whether the upstream Overages switch is ON for this account.
// "ENABLED" → true; anything else (DISABLED, UNKNOWN, empty) → false.
func isUpstreamOverageEnabled(acc config.Account) bool {
	return strings.EqualFold(acc.OverageStatus, "ENABLED")
}

func effectiveWeight(weight int) int {
	if weight < 1 {
		return 1
	}
	return weight
}
