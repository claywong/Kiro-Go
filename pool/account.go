// Package pool 账号池管理
// 实现两池分层 LRU 调度、错误冷却、Token 刷新。
//
// 两池模型:
//   - 敏感池: MinIntervalMs > 0 的账号。每次选中后,同一账号在 MinIntervalMs
//     窗口内不会再被选中。优先从敏感池 LRU 选,让窗口配额不至于空转浪费。
//   - 正常池: MinIntervalMs == 0 的账号。敏感池全在节流中时兜底,同样 LRU 选。
//
// LRU 排序基于 lastUsedAt(选中的瞬间就写),weight 作为负偏移让高权重账号
// 更"老"因此更优先。两池都空时返回 nil,由 handler 决定如何回应客户端。
package pool

import (
	"kiro-go/config"
	"strings"
	"sync"
	"time"
)

const tokenRefreshSkewSeconds int64 = 120

// quotaCooldown 是收到 429 后的账号冷却时长。MinIntervalMs 已经在做主动防御,
// 429 只是偶发抖动信号,不需要像以前那样一次 429 就把账号打入 1h 冷宫。
const quotaCooldown = 60 * time.Second

// AccountPool 账号池
type AccountPool struct {
	mu            sync.Mutex
	accounts      []config.Account
	totalAccounts int
	cooldowns     map[string]time.Time       // 账号冷却时间(429/连续错误)
	errorCounts   map[string]int             // 连续错误计数
	modelLists    map[string]map[string]bool // accountID → set of modelIDs
	lastUsedAt    map[string]time.Time       // accountID → 上次被选中的时间(LRU + MinInterval 用)
}

var (
	pool     *AccountPool
	poolOnce sync.Once
)

// GetPool 获取全局账号池单例
func GetPool() *AccountPool {
	poolOnce.Do(func() {
		pool = &AccountPool{
			cooldowns:   make(map[string]time.Time),
			errorCounts: make(map[string]int),
			modelLists:  make(map[string]map[string]bool),
			lastUsedAt:  make(map[string]time.Time),
		}
		pool.Reload()
	})
	return pool
}

// Reload 从 config 重建可路由账号列表。
// 过滤掉超额账号(除非账号级 OverageStatus=ENABLED 或全局 AllowOverUsage=true)。
// 不再做 weight 展开——LRU 通过分数偏移体现权重。
func (p *AccountPool) Reload() {
	p.mu.Lock()
	defer p.mu.Unlock()
	enabled := config.GetEnabledAccounts()
	allowOverUsage := config.GetAllowOverUsage()
	filtered := make([]config.Account, 0, len(enabled))
	for _, a := range enabled {
		if isQuotaBlocked(a, allowOverUsage) {
			continue
		}
		filtered = append(filtered, a)
	}
	p.accounts = filtered
	p.totalAccounts = len(enabled)
}

// GetNext 获取下一个可用账号
func (p *AccountPool) GetNext() *config.Account {
	return p.GetNextExcluding(nil)
}

// GetNextExcluding 获取下一个可用账号,并跳过指定账号。
func (p *AccountPool) GetNextExcluding(excluded map[string]bool) *config.Account {
	return p.pickTwoPool("", excluded)
}

// SetModelList 缓存账号支持的模型集合(由 handler 在刷新后调用)
func (p *AccountPool) SetModelList(accountID string, modelIDs []string) {
	set := make(map[string]bool, len(modelIDs))
	for _, id := range modelIDs {
		set[strings.ToLower(strings.TrimSpace(id))] = true
	}
	p.mu.Lock()
	p.modelLists[accountID] = set
	p.mu.Unlock()
}

// GetModelList 返回该账号缓存的模型 ID 列表(供 admin API 使用)。
func (p *AccountPool) GetModelList(accountID string) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
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
// 若该账号尚无模型列表(冷启动),视为支持所有模型。
// model == "" 表示不做模型过滤。调用方需持有 p.mu。
func (p *AccountPool) accountHasModel(accountID, model string) bool {
	if model == "" {
		return true
	}
	list, ok := p.modelLists[accountID]
	if !ok || len(list) == 0 {
		return true // 冷启动:列表未就绪,乐观放行
	}
	return list[strings.ToLower(strings.TrimSpace(model))]
}

// GetNextForModel 获取下一个支持指定模型的可用账号。
func (p *AccountPool) GetNextForModel(model string) *config.Account {
	return p.GetNextForModelExcluding(model, nil)
}

// GetNextForModelExcluding 获取下一个支持指定模型的可用账号,并跳过指定账号。
func (p *AccountPool) GetNextForModelExcluding(model string, excluded map[string]bool) *config.Account {
	return p.pickTwoPool(model, excluded)
}

// pickTwoPool 两池分层 LRU 选账号。敏感池优先(MinInterval>0),空则兜底正常池。
// 选中账号后立即更新 lastUsedAt——在真正发请求前,避免并发窗口撞车。
func (p *AccountPool) pickTwoPool(model string, excluded map[string]bool) *config.Account {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.accounts) == 0 {
		return nil
	}
	now := time.Now()
	allowOverUsage := config.GetAllowOverUsage()

	best := p.pickInPoolLocked(true, model, excluded, now, allowOverUsage)
	if best == nil {
		best = p.pickInPoolLocked(false, model, excluded, now, allowOverUsage)
	}
	if best != nil {
		p.lastUsedAt[best.ID] = now
	}
	return best
}

// pickInPoolLocked 在敏感池(sensitive=true)或正常池中做 LRU 选择。
// 调用方需持有 p.mu。返回值指向 p.accounts 内部,只在锁内使用。
func (p *AccountPool) pickInPoolLocked(sensitive bool, model string, excluded map[string]bool, now time.Time, allowOverUsage bool) *config.Account {
	var best *config.Account
	var bestScore time.Time
	for i := range p.accounts {
		acc := &p.accounts[i]

		// 池分类
		if sensitive {
			if acc.MinIntervalMs <= 0 {
				continue
			}
			gap := time.Duration(acc.MinIntervalMs) * time.Millisecond
			if now.Sub(p.lastUsedAt[acc.ID]) < gap {
				continue // 节流中,不可选
			}
		} else {
			if acc.MinIntervalMs > 0 {
				continue
			}
		}

		// 通用过滤
		if excluded != nil && excluded[acc.ID] {
			continue
		}
		if cd, ok := p.cooldowns[acc.ID]; ok && now.Before(cd) {
			continue
		}
		if acc.ExpiresAt > 0 && now.Unix() > acc.ExpiresAt-tokenRefreshSkewSeconds {
			continue
		}
		if isQuotaBlocked(*acc, allowOverUsage) {
			continue
		}
		if !p.accountHasModel(acc.ID, model) {
			continue
		}

		// LRU + weight 偏移:weight 越大,分数越"老",越早被选中。
		// 从未选中过的账号 lastUsedAt 是零值,自然排最前面。
		score := p.lastUsedAt[acc.ID].Add(-time.Duration(effectiveWeight(acc.Weight)) * time.Second)
		if best == nil || score.Before(bestScore) {
			best = acc
			bestScore = score
		}
	}
	return best
}

// GetByID 根据 ID 获取账号
func (p *AccountPool) GetByID(id string) *config.Account {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.accounts {
		if p.accounts[i].ID == id {
			return &p.accounts[i]
		}
	}
	return nil
}

// RecordSuccess 记录请求成功,清除冷却
func (p *AccountPool) RecordSuccess(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.cooldowns, id)
	p.errorCounts[id] = 0
}

// RecordError 记录请求错误,设置冷却
func (p *AccountPool) RecordError(id string, isQuotaError bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.errorCounts[id]++

	if isQuotaError {
		// 429:短冷却,MinInterval 是主防线
		p.cooldowns[id] = time.Now().Add(quotaCooldown)
	} else if p.errorCounts[id] >= 3 {
		// 连续 3 次错误,冷却 1 分钟
		p.cooldowns[id] = time.Now().Add(time.Minute)
	}
}

// IsAuthFailure reports whether an error indicates the refresh token / credentials
// have been revoked or invalidated upstream (401, 403 with auth markers, etc.).
func IsAuthFailure(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	lower := strings.ToLower(msg)

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

// hasStatusToken returns true when status appears in s with non-digit boundaries.
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
func IsSuspensionError(err error) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "temporarily_suspended") ||
		strings.Contains(lower, "temporarily suspended") ||
		strings.Contains(lower, "no available kiro profile")
}

// DisableAccount marks an account as disabled and removes it from the pool.
func (p *AccountPool) DisableAccount(id, reason string) {
	if err := config.SetAccountBanStatus(id, "DISABLED", reason); err != nil {
		_ = err
	}
	p.mu.Lock()
	p.cooldowns[id] = time.Now().Add(24 * time.Hour)
	p.mu.Unlock()
	p.Reload()
}

// MarkOverLimit marks an account as over usage limit (after a 402 / OVERAGE response).
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
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.totalAccounts > 0 {
		return p.totalAccounts
	}
	return len(p.accounts)
}

// AvailableCount 返回可用账号数(未在 cooldown 且未被 MinInterval 节流)。
func (p *AccountPool) AvailableCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	count := 0
	for i := range p.accounts {
		acc := &p.accounts[i]
		if cd, ok := p.cooldowns[acc.ID]; ok && now.Before(cd) {
			continue
		}
		if acc.MinIntervalMs > 0 {
			gap := time.Duration(acc.MinIntervalMs) * time.Millisecond
			if now.Sub(p.lastUsedAt[acc.ID]) < gap {
				continue
			}
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
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make([]config.Account, len(p.accounts))
	copy(result, p.accounts)
	return result
}

func isOverUsageLimit(acc config.Account) bool {
	return acc.UsageLimit > 0 && acc.UsageCurrent >= acc.UsageLimit
}

// isQuotaBlocked reports whether an over-quota account should be skipped.
func isQuotaBlocked(acc config.Account, allowOverUsage bool) bool {
	return isOverUsageLimit(acc) && !isUpstreamOverageEnabled(acc) && !allowOverUsage
}

// isUpstreamOverageEnabled reports whether upstream Overages is ON for this account.
func isUpstreamOverageEnabled(acc config.Account) bool {
	return strings.EqualFold(acc.OverageStatus, "ENABLED")
}

func effectiveWeight(weight int) int {
	if weight < 1 {
		return 1
	}
	return weight
}
