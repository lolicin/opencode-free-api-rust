// 出口代理池：上游限流（RPM 与时间窗额度）按出口 IP 判定，用代理池把请求
// 摊到多个出口 IP 上。轮询选路；某个出口被限流（429）时就地冷却，
// 冷却期内不再选中它，请求改由其余代理承担。
use std::{
    env,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
    time::{Duration, Instant},
};

use ureq::Proxy;

#[derive(Debug, Clone)]
pub struct ProxyEntry {
    pub url: String,
    pub proxy: Proxy,
}

pub struct ProxyPool {
    // 出口列表不可变，构造后只读。
    entries: Vec<ProxyEntry>,
    // 与 entries 同下标的冷却截止时间；None 表示可用。
    cooldowns: Mutex<Vec<Option<Instant>>>,
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
            next: AtomicUsize::new(0),
            cooldown,
        }
    }

    pub fn from_env() -> Self {
        let cooldown = env_seconds("PROXY_COOLDOWN", 60);
        Self::new(
            parse_proxies(&env::var("PROXIES").unwrap_or_default()),
            cooldown,
        )
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

    // 按下标取出口，用于预建每出口独立的 Agent。
    pub fn proxy_at(&self, index: usize) -> Proxy {
        self.entries[index].proxy.clone()
    }

    // 轮询挑一个当前不在冷却期的出口，返回下标与 Proxy 克隆。
    // 全部处于冷却时退化为「选冷却最早结束的那个」，避免一次集中限流就彻底断流。
    pub fn acquire(&self) -> Option<(usize, Proxy)> {
        if self.entries.is_empty() {
            return None;
        }
        let len = self.entries.len();
        let start = self.next.fetch_add(1, Ordering::Relaxed) % len;
        let now = Instant::now();
        let cooldowns = self.cooldowns.lock().expect("proxy pool lock");
        let mut fallback: Option<(usize, Instant)> = None;
        for offset in 0..len {
            let index = (start + offset) % len;
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
                _ => return Some((index, self.entries[index].proxy.clone())),
            }
        }
        fallback.map(|(index, _)| (index, self.entries[index].proxy.clone()))
    }

    // 标记某个出口被限流，进入冷却期。冷却只延长不缩短，避免重复命中反复重置计时。
    pub fn penalize(&self, index: usize) {
        let mut cooldowns = self.cooldowns.lock().expect("proxy pool lock");
        if index >= cooldowns.len() {
            return;
        }
        let until = Instant::now() + self.cooldown;
        let should_extend = match cooldowns[index] {
            Some(existing) => until > existing,
            None => true,
        };
        if should_extend {
            cooldowns[index] = Some(until);
        }
    }

    // 当前可用（不在冷却期）的出口数，用于可观测性。
    pub fn available(&self) -> usize {
        let now = Instant::now();
        let cooldowns = self.cooldowns.lock().expect("proxy pool lock");
        cooldowns
            .iter()
            .filter(|slot| match slot {
                Some(until) => *until <= now,
                None => true,
            })
            .count()
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
                proxy,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(url: &str) -> ProxyEntry {
        ProxyEntry {
            url: url.to_owned(),
            proxy: Proxy::new(url).expect("valid proxy url"),
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
        let picked: Vec<usize> = (0..6).map(|_| pool.acquire().unwrap().0).collect();
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
            assert_eq!(pool.acquire().unwrap().0, 1);
        }
        std::thread::sleep(Duration::from_millis(200));
        // 冷却结束后重新参与轮询。
        assert_eq!(pool.available(), 2);
        let picked: Vec<usize> = (0..2).map(|_| pool.acquire().unwrap().0).collect();
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
}
