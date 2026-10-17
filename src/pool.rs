// 出口池：上游限流（RPM 与时间窗额度）按出口 IP 判定，用多个出口把请求摊开。
// 轮询选路；某个出口被限流（429）时就地冷却，冷却期内不再选中它，
// 请求改由其余出口承担。出口可以是直连（proxy = None），也可以是代理。
use std::{
    env,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
    time::{Duration, Instant},
};

use ureq::Proxy;

// 一个出口。proxy 为 None 表示直连（本机出口 IP）。
#[derive(Debug, Clone)]
pub struct ProxyEntry {
    pub url: String,
    pub proxy: Option<Proxy>,
}

impl ProxyEntry {
    // 直连出口，显示名用于日志。
    pub fn direct() -> Self {
        Self {
            url: "direct".to_owned(),
            proxy: None,
        }
    }

    pub fn is_direct(&self) -> bool {
        self.proxy.is_none()
    }
}

pub struct ProxyPool {
    // 出口列表不可变，构造后只读。
    entries: Vec<ProxyEntry>,
    // 与 entries 同下标的冷却截止时间；None 表示可用。
    cooldowns: Mutex<Vec<Option<Instant>>>,
    // 永久剔除的出口下标（403 FreeTierError，等也不会恢复）。
    retired: Mutex<Vec<usize>>,
    // 轮询游标，跨工作线程共享。
    next: AtomicUsize,
    cooldown: Duration,
}

impl ProxyPool {
    pub fn new(entries: Vec<ProxyEntry>, cooldown: Duration) -> Self {
        let cooldowns = vec![None; entries.len()];
        Self {
            entries,
            cooldowns: Mutex::new(cooldowns),
            retired: Mutex::new(Vec::new()),
            next: AtomicUsize::new(0),
            cooldown,
        }
    }

    pub fn from_env() -> Self {
        let cooldown = env_seconds("PROXY_COOLDOWN", 60);
        let proxies = parse_proxies(&env::var("PROXIES").unwrap_or_default());
        // INCLUDE_DIRECT=1 时把直连也作为一个出口，和代理一起轮询：
        // 平时直连、直连被限流（429）后自动切到代理，冷却结束再回来。
        let include_direct = env_flag("INCLUDE_DIRECT");
        let mut entries = Vec::with_capacity(proxies.len() + 1);
        if include_direct {
            entries.push(ProxyEntry::direct());
        }
        entries.extend(proxies);
        Self::new(entries, cooldown)
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn cooldown(&self) -> Duration {
        self.cooldown
    }

    pub fn urls(&self) -> Vec<String> {
        self.entries.iter().map(|entry| entry.url.clone()).collect()
    }

    // 按下标取出口，用于预建每出口独立的 Agent。None 表示直连。
    pub fn proxy_at(&self, index: usize) -> Option<Proxy> {
        self.entries[index].proxy.clone()
    }

    pub fn entry_at(&self, index: usize) -> &ProxyEntry {
        &self.entries[index]
    }

    // 轮询挑一个当前不在冷却期的出口，返回下标。
    // 全部处于冷却时退化为「选冷却最早结束的那个」，避免一次集中限流就彻底断流。
    pub fn acquire(&self) -> Option<usize> {
        if self.entries.is_empty() {
            return None;
        }
        let len = self.entries.len();
        let start = self.next.fetch_add(1, Ordering::Relaxed) % len;
        let now = Instant::now();
        let retired = self.retired.lock().expect("proxy pool lock").clone();
        // 所有出口都被剔除时不再拦截，交给上游裁决，避免把自己锁死成完全不可用。
        let all_retired = retired.len() >= len;
        let cooldowns = self.cooldowns.lock().expect("proxy pool lock");
        let mut fallback: Option<(usize, Instant)> = None;
        for offset in 0..len {
            let index = (start + offset) % len;
            if !all_retired && retired.contains(&index) {
                continue;
            }
            match cooldowns[index] {
                Some(until) if until > now => {
                    let earlier = match fallback {
                        Some((_, best)) => until < best,
                        None => true,
                    };
                    if earlier {
                        fallback = Some((index, until));
                    }
                }
                _ => return Some(index),
            }
        }
        fallback.map(|(index, _)| index)
    }

    // 标记某个出口被限流，进入冷却期。冷却只延长不缩短，避免重复命中反复重置计时。
    pub fn penalize(&self, index: usize) {
        self.penalize_for(index, self.cooldown);
    }

    // 按指定时长冷却。上游 429 会给出 Retry-After（实测指向下一个 UTC 0 点），
    // 用它比默认时常更准；拿不到时调用方回落到 penalize。
    pub fn penalize_for(&self, index: usize, duration: Duration) {
        let mut cooldowns = self.cooldowns.lock().expect("proxy pool lock");
        if index >= cooldowns.len() {
            return;
        }
        let until = Instant::now() + duration;
        let should_extend = match cooldowns[index] {
            Some(existing) => until > existing,
            None => true,
        };
        if should_extend {
            cooldowns[index] = Some(until);
        }
    }

    // 永久剔除某个出口。用于 403 FreeTierError：这是出口 IP 类型不合格
    // （实测机房 IP 必挂），等多久都不会恢复，继续轮询只会浪费请求。
    pub fn retire(&self, index: usize) {
        let mut retired = self.retired.lock().expect("proxy pool lock");
        if index < self.entries.len() && !retired.contains(&index) {
            retired.push(index);
        }
    }

    pub fn is_retired(&self, index: usize) -> bool {
        self.retired
            .lock()
            .expect("proxy pool lock")
            .contains(&index)
    }

    pub fn retired_count(&self) -> usize {
        self.retired.lock().expect("proxy pool lock").len()
    }

    // 当前可用（不在冷却期、且未被剔除）的出口数，用于可观测性。
    pub fn available(&self) -> usize {
        let now = Instant::now();
        let retired = self.retired.lock().expect("proxy pool lock").clone();
        let cooldowns = self.cooldowns.lock().expect("proxy pool lock");
        cooldowns
            .iter()
            .enumerate()
            .filter(|(index, slot)| {
                if retired.contains(index) {
                    return false;
                }
                match slot {
                    Some(until) => *until <= now,
                    None => true,
                }
            })
            .count()
    }

    // 距离该出口恢复还要多久，供日志与 /health 使用。
    pub fn cooldown_remaining(&self, index: usize) -> Option<Duration> {
        let cooldowns = self.cooldowns.lock().expect("proxy pool lock");
        let until = (*cooldowns.get(index)?)?;
        let now = Instant::now();
        if until > now {
            Some(until - now)
        } else {
            None
        }
    }
}

// 解析 PROXIES：逗号分隔，支持 http/https/socks5 及 host:port 简写。
// 无法解析的条目直接丢弃（并保留其余可用出口），排序去重保证轮询顺序稳定。
pub fn parse_proxies(value: &str) -> Vec<ProxyEntry> {
    let mut entries: Vec<ProxyEntry> = value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .filter_map(|item| {
            Proxy::new(item).ok().map(|proxy| ProxyEntry {
                url: item.to_owned(),
                proxy: Some(proxy),
            })
        })
        .collect();
    entries.sort_by(|a, b| a.url.cmp(&b.url));
    entries.dedup_by(|a, b| a.url == b.url);
    entries
}

pub fn env_seconds(name: &str, fallback: u64) -> Duration {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(fallback))
}

pub fn env_flag(name: &str) -> bool {
    env::var(name)
        .map(|value| {
            matches!(
                value.to_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(url: &str) -> ProxyEntry {
        ProxyEntry {
            url: url.to_owned(),
            proxy: Some(Proxy::new(url).expect("valid proxy url")),
        }
    }

    #[test]
    fn parses_and_dedups_proxy_list() {
        let entries = parse_proxies(
            " http://a.example:8080 , http://b.example:8080 , http://a.example:8080 , ",
        );
        assert_eq!(
            entries.iter().map(|e| e.url.as_str()).collect::<Vec<_>>(),
            vec!["http://a.example:8080", "http://b.example:8080"]
        );
    }

    #[test]
    fn skips_unparsable_entries_but_keeps_valid_ones() {
        let entries = parse_proxies("http://a.example:8080, ::bad::, http://b.example:8080");
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn empty_config_yields_empty_pool() {
        let pool = ProxyPool::new(parse_proxies("  ,  "), Duration::from_secs(60));
        assert!(pool.is_empty());
        assert!(pool.acquire().is_none());
    }

    #[test]
    fn round_robins_across_exits() {
        let pool = ProxyPool::new(
            parse_proxies("http://a.example:1,http://b.example:2,http://c.example:3"),
            Duration::from_secs(60),
        );
        let picked: Vec<usize> = (0..6).map(|_| pool.acquire().unwrap()).collect();
        // 轮询：每个出口各命中两次，顺序在池内循环。
        assert_eq!(picked, vec![0, 1, 2, 0, 1, 2]);
    }

    #[test]
    fn penalized_exit_is_skipped_until_cooldown_expires() {
        let pool = ProxyPool::new(
            parse_proxies("http://a.example:1,http://b.example:2"),
            Duration::from_millis(150),
        );
        pool.penalize(0);
        assert_eq!(pool.available(), 1);
        // 冷却中的 0 号被跳过，只挑 1 号。
        for _ in 0..4 {
            assert_eq!(pool.acquire().unwrap(), 1);
        }
        std::thread::sleep(Duration::from_millis(200));
        // 冷却结束后重新参与轮询。
        assert_eq!(pool.available(), 2);
        let picked: Vec<usize> = (0..2).map(|_| pool.acquire().unwrap()).collect();
        assert!(picked.contains(&0), "exit 0 should be back, got {picked:?}");
    }

    #[test]
    fn all_cooling_falls_back_to_earliest_recovery() {
        let pool = ProxyPool::new(
            parse_proxies("http://a.example:1,http://b.example:2"),
            Duration::from_secs(60),
        );
        pool.penalize(0);
        pool.penalize(1);
        assert_eq!(pool.available(), 0);
        // 全部冷却时退化为选「最早恢复」的那个，而不是直接断流。
        assert!(pool.acquire().is_some());
    }

    #[test]
    fn repeated_penalty_never_shortens_cooldown() {
        let pool = ProxyPool::new(
            parse_proxies("http://a.example:1"),
            Duration::from_millis(300),
        );
        pool.penalize(0);
        let first = pool.cooldowns.lock().unwrap()[0];
        std::thread::sleep(Duration::from_millis(150));
        pool.penalize(0);
        let second = pool.cooldowns.lock().unwrap()[0];
        assert!(
            second >= first,
            "cooldown must only extend, never shrink: {first:?} -> {second:?}"
        );
    }

    #[test]
    fn penalize_out_of_range_is_ignored() {
        let pool = ProxyPool::new(parse_proxies("http://a.example:1"), Duration::from_secs(60));
        pool.penalize(99);
        assert_eq!(pool.available(), 1);
    }

    // 直连作为一个出口：直连优先，被限流后自动切到代理，冷却结束再回来。
    #[test]
    fn direct_exit_prefers_direct_then_fails_over_to_proxy() {
        let mut entries = vec![ProxyEntry::direct()];
        entries.extend(parse_proxies("http://a.example:1"));
        let pool = ProxyPool::new(entries, Duration::from_millis(150));

        assert!(pool.entry_at(0).is_direct());
        assert!(pool.proxy_at(0).is_none());
        assert!(pool.proxy_at(1).is_some());

        // 直连被限流后，请求改走代理出口。
        pool.penalize(0);
        for _ in 0..3 {
            assert_eq!(pool.acquire().unwrap(), 1);
        }
        assert_eq!(pool.available(), 1);

        // 冷却结束后直连重新参与轮询。
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(pool.available(), 2);
        let picked: Vec<usize> = (0..2).map(|_| pool.acquire().unwrap()).collect();
        assert!(picked.contains(&0), "direct should be back, got {picked:?}");
    }

    // 未开启 INCLUDE_DIRECT 时，池里只有代理，不会回落到直连。
    #[test]
    fn direct_is_absent_unless_enabled() {
        let entries = parse_proxies("http://a.example:1");
        assert!(entries.iter().all(|e| !e.is_direct()));
    }

    // 上游给 Retry-After（实测 28699 秒 → 下一个 UTC 0 点）时按它冷却。
    #[test]
    fn penalize_for_honours_upstream_retry_after() {
        let pool = ProxyPool::new(
            parse_proxies("http://a.example:1,http://b.example:2"),
            Duration::from_secs(60),
        );
        pool.penalize_for(0, Duration::from_secs(28699));
        // 冷却时长应远超默认的 60 秒。
        let remaining = pool.cooldown_remaining(0).expect("should be cooling");
        assert!(
            remaining > Duration::from_secs(28000),
            "expected ~28699s cooldown, got {remaining:?}"
        );
        // 冷却中的出口不被选中。
        for _ in 0..4 {
            assert_eq!(pool.acquire().unwrap(), 1);
        }
        assert_eq!(pool.available(), 1);
    }

    #[test]
    fn cooldown_expires_and_exit_returns() {
        let pool = ProxyPool::new(
            parse_proxies("http://a.example:1,http://b.example:2"),
            Duration::from_secs(60),
        );
        pool.penalize_for(0, Duration::from_millis(120));
        assert_eq!(pool.available(), 1);
        assert!(pool.cooldown_remaining(0).is_some());
        std::thread::sleep(Duration::from_millis(180));
        assert_eq!(pool.available(), 2);
        assert!(pool.cooldown_remaining(0).is_none(), "cooldown should be over");
        let picked: Vec<usize> = (0..2).map(|_| pool.acquire().unwrap()).collect();
        assert!(picked.contains(&0));
    }

    // 403 FreeTierError 是 IP 类型不合格，等待无用 → 永久剔除。
    #[test]
    fn retired_exit_is_never_selected() {
        let pool = ProxyPool::new(
            parse_proxies("http://dc.example:1,http://home.example:2"),
            Duration::from_secs(60),
        );
        pool.retire(0);
        assert!(pool.is_retired(0));
        assert_eq!(pool.retired_count(), 1);
        assert_eq!(pool.available(), 1);
        for _ in 0..5 {
            assert_eq!(pool.acquire().unwrap(), 1, "retired exit must not be picked");
        }
    }

    #[test]
    fn retiring_the_same_exit_twice_is_idempotent() {
        let pool = ProxyPool::new(
            parse_proxies("http://a.example:1,http://b.example:2"),
            Duration::from_secs(60),
        );
        pool.retire(0);
        pool.retire(0);
        assert_eq!(pool.retired_count(), 1);
    }

    // 全部出口都被剔除时不能把池子锁死，仍要给出一个出口交给上游裁决。
    #[test]
    fn all_retired_still_yields_an_exit() {
        let pool = ProxyPool::new(
            parse_proxies("http://a.example:1,http://b.example:2"),
            Duration::from_secs(60),
        );
        pool.retire(0);
        pool.retire(1);
        assert_eq!(pool.available(), 0);
        assert!(pool.acquire().is_some(), "must not deadlock when all retired");
    }

    #[test]
    fn retire_out_of_range_is_ignored() {
        let pool = ProxyPool::new(parse_proxies("http://a.example:1"), Duration::from_secs(60));
        pool.retire(42);
        assert_eq!(pool.retired_count(), 0);
        assert_eq!(pool.available(), 1);
    }
}
