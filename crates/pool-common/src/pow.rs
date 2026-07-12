use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Result};
use blocknet_pow_kernel::{FixedArgon2id, PowBlock};

const POW_MEMORY_KIB: u32 = 2 * 1024 * 1024;

/// The default validator has one candidate worker, two regular workers, and one audit worker.
/// Retaining more arenas than that would keep additional 2 GiB buffers resident after a burst.
pub const DEFAULT_POW_ARENA_CACHE_LIMIT: usize = 4;

pub trait PowHasher: Send + Sync + 'static {
    fn hash(&self, header_base: &[u8], nonce: u64) -> Result<[u8; 32]>;
}

/// Snapshot of the reusable PoW arena cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PowArenaPoolStats {
    pub allocations: u64,
    pub cache_hits: u64,
    pub discarded: u64,
    pub cached_arenas: usize,
    pub retained_limit: usize,
    /// Allocations backed by explicit HugeTLB pages (pre-reserved 2 MiB pool).
    pub hugetlb_allocations: u64,
    /// Allocations backed by an ordinary mapping with a transparent-hugepage hint.
    pub thp_hint_allocations: u64,
    /// Allocations that fell back to ordinary heap memory.
    pub heap_allocations: u64,
}

/// How a checked-out arena's 2 GiB of memory is backed. TLB pressure is the
/// dominant cost of an Argon2id verify on x86_64, so explicit hugepages cut
/// verify latency by roughly a fifth when the host pre-reserves them
/// (`vm.nr_hugepages`). Allocation always falls back — verification must
/// never fail because a host has no hugepage pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArenaBacking {
    ExplicitHugeTlb,
    ThpHint,
    Heap,
}

/// Consensus-compatible Argon2id PoW backed by reusable 2 GiB memory arenas.
///
/// Clones share the same arena pool. Concurrent hashes allocate independent arenas, but at most
/// [`DEFAULT_POW_ARENA_CACHE_LIMIT`] are retained after those hashes finish.
#[derive(Clone)]
pub struct Argon2PowHasher {
    kernel: Arc<PowKernel>,
}

impl fmt::Debug for Argon2PowHasher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Argon2PowHasher")
            .field("memory_kib", &self.kernel.memory_kib)
            .field("isa", &self.kernel.hasher.isa_label())
            .field("arena_pool", &self.arena_pool_stats())
            .finish()
    }
}

impl Default for Argon2PowHasher {
    fn default() -> Self {
        Self::new(POW_MEMORY_KIB, DEFAULT_POW_ARENA_CACHE_LIMIT)
    }
}

impl Argon2PowHasher {
    fn new(memory_kib: u32, retained_limit: usize) -> Self {
        Self {
            kernel: Arc::new(PowKernel::new(memory_kib, retained_limit)),
        }
    }

    /// Returns allocation and cache counters for operational diagnostics.
    pub fn arena_pool_stats(&self) -> PowArenaPoolStats {
        self.kernel.arenas.stats()
    }
}

impl PowHasher for Argon2PowHasher {
    fn hash(&self, header_base: &[u8], nonce: u64) -> Result<[u8; 32]> {
        self.kernel.hash(header_base, nonce)
    }
}

struct PowKernel {
    memory_kib: u32,
    hasher: FixedArgon2id,
    arenas: PowArenaPool,
}

impl PowKernel {
    fn new(memory_kib: u32, retained_limit: usize) -> Self {
        Self {
            memory_kib,
            hasher: FixedArgon2id::new(memory_kib),
            arenas: PowArenaPool::new(retained_limit),
        }
    }

    fn hash(&self, header_base: &[u8], nonce: u64) -> Result<[u8; 32]> {
        let mut arena = self.arenas.checkout(self.hasher.block_count())?;
        let mut output = [0u8; 32];
        self.hasher
            .hash_password_into_with_memory(
                &nonce.to_le_bytes(),
                header_base,
                &mut output,
                arena.deref_mut(),
            )
            .map_err(|err| anyhow!("argon2 hash failure: {err:?}"))?;
        Ok(output)
    }
}

struct PowArenaPool {
    available: Mutex<Vec<PowArena>>,
    retained_limit: usize,
    allocations: AtomicU64,
    cache_hits: AtomicU64,
    discarded: AtomicU64,
    hugetlb_allocations: AtomicU64,
    thp_hint_allocations: AtomicU64,
    heap_allocations: AtomicU64,
}

impl PowArenaPool {
    fn new(retained_limit: usize) -> Self {
        Self {
            available: Mutex::new(Vec::with_capacity(retained_limit)),
            retained_limit,
            allocations: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            discarded: AtomicU64::new(0),
            hugetlb_allocations: AtomicU64::new(0),
            thp_hint_allocations: AtomicU64::new(0),
            heap_allocations: AtomicU64::new(0),
        }
    }

    fn checkout(&self, block_count: usize) -> Result<PowArenaGuard<'_>> {
        let cached = {
            let mut available = self
                .available
                .lock()
                .map_err(|_| anyhow!("PoW arena cache lock poisoned"))?;
            available.pop()
        };

        let arena = if let Some(arena) = cached {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            arena
        } else {
            // A 2 GiB allocation can be slow. Keep it outside the cache lock so another hash can
            // return or check out an arena concurrently.
            let arena = allocate_arena(block_count)?;
            self.allocations.fetch_add(1, Ordering::Relaxed);
            let backing_counter = match arena.backing() {
                ArenaBacking::ExplicitHugeTlb => &self.hugetlb_allocations,
                ArenaBacking::ThpHint => &self.thp_hint_allocations,
                ArenaBacking::Heap => &self.heap_allocations,
            };
            backing_counter.fetch_add(1, Ordering::Relaxed);
            arena
        };

        Ok(PowArenaGuard {
            pool: self,
            arena: Some(arena),
        })
    }

    fn stats(&self) -> PowArenaPoolStats {
        let cached_arenas = self
            .available
            .lock()
            .map(|arenas| arenas.len())
            .unwrap_or_default();
        PowArenaPoolStats {
            allocations: self.allocations.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            discarded: self.discarded.load(Ordering::Relaxed),
            cached_arenas,
            retained_limit: self.retained_limit,
            hugetlb_allocations: self.hugetlb_allocations.load(Ordering::Relaxed),
            thp_hint_allocations: self.thp_hint_allocations.load(Ordering::Relaxed),
            heap_allocations: self.heap_allocations.load(Ordering::Relaxed),
        }
    }
}

/// A verification arena: either an mmap-backed region (Linux; explicit
/// HugeTLB or an ordinary mapping with a THP hint) or plain heap memory.
enum PowArena {
    #[cfg(target_os = "linux")]
    Mmap(MmapPowArena),
    Heap(Vec<PowBlock>),
}

impl PowArena {
    fn backing(&self) -> ArenaBacking {
        match self {
            #[cfg(target_os = "linux")]
            PowArena::Mmap(arena) => arena.backing,
            PowArena::Heap(_) => ArenaBacking::Heap,
        }
    }

    fn as_mut_slice(&mut self) -> &mut [PowBlock] {
        match self {
            #[cfg(target_os = "linux")]
            PowArena::Mmap(arena) => arena.as_mut_slice(),
            PowArena::Heap(arena) => arena.as_mut_slice(),
        }
    }

    fn as_slice(&self) -> &[PowBlock] {
        match self {
            #[cfg(target_os = "linux")]
            PowArena::Mmap(arena) => arena.as_slice(),
            PowArena::Heap(arena) => arena.as_slice(),
        }
    }
}

/// Best-effort hugepage-backed arena. Every path is fail-open: a host with no
/// reserved hugepage pool silently gets THP-hinted or heap memory instead.
#[cfg(target_os = "linux")]
struct MmapPowArena {
    ptr: std::ptr::NonNull<PowBlock>,
    mapped_len: usize,
    block_count: usize,
    backing: ArenaBacking,
}

// Safety: the arena exclusively owns its anonymous private mapping; nothing
// else aliases it, so moving/sharing it across threads is sound.
#[cfg(target_os = "linux")]
unsafe impl Send for MmapPowArena {}
#[cfg(target_os = "linux")]
unsafe impl Sync for MmapPowArena {}

#[cfg(target_os = "linux")]
impl MmapPowArena {
    const HUGE_2M: usize = 2 * 1024 * 1024;

    fn try_new(block_count: usize) -> Option<Self> {
        let byte_len = block_count.checked_mul(std::mem::size_of::<PowBlock>())?;

        // Attempt 1: explicit HugeTLB pages from the pre-reserved pool. The
        // mapping length must be a hugepage multiple; MAP_POPULATE surfaces
        // reservation shortfalls here instead of as a later SIGBUS.
        let hugetlb_len = byte_len.checked_add(Self::HUGE_2M - 1)? / Self::HUGE_2M * Self::HUGE_2M;
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                hugetlb_len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_HUGETLB | libc::MAP_POPULATE,
                -1,
                0,
            )
        };
        if ptr != libc::MAP_FAILED {
            return Some(Self {
                ptr: std::ptr::NonNull::new(ptr as *mut PowBlock)?,
                mapped_len: hugetlb_len,
                block_count,
                backing: ArenaBacking::ExplicitHugeTlb,
            });
        }

        // Attempt 2: ordinary anonymous mapping with a THP hint. Coverage is
        // up to the kernel; still zero-cost to request.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                byte_len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return None;
        }
        unsafe {
            libc::madvise(ptr, byte_len, libc::MADV_HUGEPAGE);
        }
        Some(Self {
            ptr: std::ptr::NonNull::new(ptr as *mut PowBlock)?,
            mapped_len: byte_len,
            block_count,
            backing: ArenaBacking::ThpHint,
        })
    }

    fn as_mut_slice(&mut self) -> &mut [PowBlock] {
        // Safety: the mapping is at least block_count * size_of::<PowBlock>()
        // bytes, page alignment satisfies PowBlock's alignment, and anonymous
        // mappings are zero-initialized, which is a valid PowBlock bit
        // pattern (a plain u64 array).
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.block_count) }
    }

    fn as_slice(&self) -> &[PowBlock] {
        // Safety: as above.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.block_count) }
    }
}

#[cfg(target_os = "linux")]
impl Drop for MmapPowArena {
    fn drop(&mut self) {
        // Safety: ptr/mapped_len describe exactly one live mapping owned by
        // this arena.
        unsafe {
            libc::munmap(self.ptr.as_ptr() as *mut libc::c_void, self.mapped_len);
        }
    }
}

fn allocate_arena(block_count: usize) -> Result<PowArena> {
    #[cfg(target_os = "linux")]
    if let Some(arena) = MmapPowArena::try_new(block_count) {
        return Ok(PowArena::Mmap(arena));
    }

    let mut arena = Vec::new();
    arena
        .try_reserve_exact(block_count)
        .map_err(|err| anyhow!("failed reserving PoW arena with {block_count} blocks: {err}"))?;
    arena.resize(block_count, PowBlock::default());
    Ok(PowArena::Heap(arena))
}

struct PowArenaGuard<'a> {
    pool: &'a PowArenaPool,
    arena: Option<PowArena>,
}

impl Deref for PowArenaGuard<'_> {
    type Target = [PowBlock];

    fn deref(&self) -> &Self::Target {
        self.arena
            .as_ref()
            .expect("PoW arena guard must always hold an arena")
            .as_slice()
    }
}

impl DerefMut for PowArenaGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.arena
            .as_mut()
            .expect("PoW arena guard must always hold an arena")
            .as_mut_slice()
    }
}

impl Drop for PowArenaGuard<'_> {
    fn drop(&mut self) {
        let Some(arena) = self.arena.take() else {
            return;
        };
        let mut arena = Some(arena);
        let cached = match self.pool.available.lock() {
            Ok(mut available) if available.len() < self.pool.retained_limit => {
                available.push(
                    arena
                        .take()
                        .expect("arena must be present until returned to cache"),
                );
                true
            }
            _ => false,
        };
        if !cached {
            self.pool.discarded.fetch_add(1, Ordering::Relaxed);
        }
        // If the cache is full (or poisoned), the arena is freed here after releasing its lock.
        drop(arena);
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DeterministicTestHasher;

impl PowHasher for DeterministicTestHasher {
    fn hash(&self, header_base: &[u8], nonce: u64) -> Result<[u8; 32]> {
        // Cheap deterministic hash for tests: nonce-seeded rolling mixer over header bytes.
        let mut s0 = nonce.wrapping_mul(0x9E3779B97F4A7C15);
        let mut s1 = (!nonce).wrapping_mul(0xBF58476D1CE4E5B9);
        for &b in header_base {
            s0 ^= (b as u64).wrapping_mul(0x100000001B3);
            s0 = s0.rotate_left(13).wrapping_add(s1);
            s1 ^= s0.rotate_right(7);
        }

        let mut out = [0u8; 32];
        for i in 0..4 {
            s0 ^= s1.rotate_left(17);
            s1 ^= s0.rotate_right(11);
            let v = s0
                .wrapping_add(s1)
                .wrapping_add((i as u64).wrapping_mul(0x9E3779B97F4A7C15));
            out[i * 8..(i + 1) * 8].copy_from_slice(&v.to_be_bytes());
        }
        Ok(out)
    }
}

pub fn check_target(hash: [u8; 32], target: [u8; 32]) -> bool {
    for i in 0..32 {
        if hash[i] < target[i] {
            return true;
        }
        if hash[i] > target[i] {
            return false;
        }
    }
    true
}

pub fn difficulty_to_target(difficulty: u64) -> [u8; 32] {
    if difficulty == 0 {
        return [0u8; 32];
    }

    let numerator = [u64::MAX; 4];
    let mut quotient = [0u64; 4];
    let mut rem = 0u64;

    for (i, limb) in numerator.into_iter().enumerate() {
        let high = rem as u128;
        let low = limb as u128;
        let dividend = (high << 64) | low;
        quotient[i] = (dividend / difficulty as u128) as u64;
        rem = (dividend % difficulty as u128) as u64;
    }

    let mut target = [0u8; 32];
    for (i, word) in quotient.into_iter().enumerate() {
        target[i * 8..(i + 1) * 8].copy_from_slice(&word.to_be_bytes());
    }
    target
}

#[cfg(test)]
mod tests {
    use argon2::{Algorithm, Argon2, Params, Version};

    use super::*;

    fn reference_pow_hash(header: &[u8], nonce: u64, memory_kib: u32) -> [u8; 32] {
        let params = Params::new(memory_kib, 1, 1, Some(32))
            .expect("reference Argon2 parameters should be valid");
        let reference = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
        let mut memory = vec![argon2::Block::default(); reference.params().block_count()];
        let mut output = [0u8; 32];
        reference
            .hash_password_into_with_memory(&nonce.to_le_bytes(), header, &mut output, &mut memory)
            .expect("reference hash should succeed");
        output
    }

    #[test]
    fn fixed_kernel_matches_reference_for_small_memory() {
        let headers = [
            b"12345678".as_slice(),
            b"test_block_header_data".as_slice(),
            b"headerbase0123456789abcdefghijklmnop".as_slice(),
        ];
        let nonces = [0u64, 42u64, 1_000_003u64];

        for memory_kib in [8u32, 32u32, 4096u32] {
            let hasher = Argon2PowHasher::new(memory_kib, 1);
            for header in headers {
                for nonce in nonces {
                    let expected = reference_pow_hash(header, nonce, memory_kib);
                    let actual = hasher
                        .hash(header, nonce)
                        .expect("fixed hash should succeed");
                    assert_eq!(
                        actual, expected,
                        "mismatch for memory_kib={memory_kib} nonce={nonce}"
                    );
                }
            }
        }
    }

    fn assert_backing_sum(stats: &PowArenaPoolStats) {
        assert_eq!(
            stats.hugetlb_allocations + stats.thp_hint_allocations + stats.heap_allocations,
            stats.allocations,
            "every allocation must be attributed to exactly one backing class"
        );
    }

    #[test]
    fn arena_is_reused_across_hashes() {
        let hasher = Argon2PowHasher::new(32, 1);
        let header = b"reusable-arena-header";

        hasher.hash(header, 1).expect("first hash");
        let stats = hasher.arena_pool_stats();
        assert_eq!(stats.allocations, 1);
        assert_eq!(stats.cache_hits, 0);
        assert_eq!(stats.discarded, 0);
        assert_eq!(stats.cached_arenas, 1);
        assert_eq!(stats.retained_limit, 1);
        assert_backing_sum(&stats);

        hasher.hash(header, 2).expect("second hash");
        let stats = hasher.arena_pool_stats();
        assert_eq!(stats.allocations, 1);
        assert_eq!(stats.cache_hits, 1);
        assert_eq!(stats.discarded, 0);
        assert_eq!(stats.cached_arenas, 1);
        assert_backing_sum(&stats);
    }

    #[test]
    fn cache_discards_arenas_beyond_its_retained_limit() {
        let hasher = Argon2PowHasher::new(8, 2);
        let first = hasher
            .kernel
            .arenas
            .checkout(hasher.kernel.hasher.block_count())
            .expect("first arena");
        let second = hasher
            .kernel
            .arenas
            .checkout(hasher.kernel.hasher.block_count())
            .expect("second arena");
        let third = hasher
            .kernel
            .arenas
            .checkout(hasher.kernel.hasher.block_count())
            .expect("third arena");

        drop(first);
        drop(second);
        drop(third);

        let stats = hasher.arena_pool_stats();
        assert_eq!(stats.allocations, 3);
        assert_eq!(stats.cache_hits, 0);
        assert_eq!(stats.discarded, 1);
        assert_eq!(stats.cached_arenas, 2);
        assert_eq!(stats.retained_limit, 2);
        assert_backing_sum(&stats);
    }

    #[test]
    fn arena_allocation_never_fails_without_hugepages() {
        // The whole point of the fail-open chain: any host, any configuration,
        // an arena is always produced and hashes still verify.
        let arena = allocate_arena(64).expect("fail-open allocation");
        assert_eq!(arena.as_slice().len(), 64);
        let hasher = Argon2PowHasher::new(8, 1);
        hasher.hash(b"fail-open", 7).expect("hash on any backing");
        let stats = hasher.arena_pool_stats();
        assert_backing_sum(&stats);
    }

    #[test]
    #[ignore = "explicit production-profile timing probe; hashes 2 GiB arenas repeatedly"]
    fn production_profile_verify_latency_probe() {
        // Run with --ignored --nocapture, with and without reserved 2 MiB
        // hugepages (vm.nr_hugepages), to measure the verify-latency effect.
        let hasher = Argon2PowHasher::default();
        let header = b"blocknet-pool-verify-latency-probe";
        let warmup = hasher.hash(header, 0).expect("warmup hash");
        let start = std::time::Instant::now();
        let rounds = 5u64;
        for nonce in 1..=rounds {
            hasher.hash(header, nonce).expect("probe hash");
        }
        let per_hash = start.elapsed() / rounds as u32;
        println!(
            "verify latency: {per_hash:?}/hash over {rounds} hashes; stats {:?}; warmup byte {}",
            hasher.arena_pool_stats(),
            warmup[0],
        );
    }

    #[test]
    #[ignore = "explicit production-profile test; runs two sequential 2 GiB Argon2 hashes"]
    fn production_profile_matches_argon2_reference() {
        let header = b"blocknet-pool-production-vector-v1";
        let nonce = 0x0102_0304_0506_0708;
        let expected = reference_pow_hash(header, nonce, POW_MEMORY_KIB);
        let hasher = Argon2PowHasher::new(POW_MEMORY_KIB, 0);
        let actual = hasher
            .hash(header, nonce)
            .expect("production fixed hash should succeed");
        assert_eq!(actual, expected);
    }

    #[test]
    fn check_target_orders_big_endian() {
        let mut hash = [0x10u8; 32];
        let mut target = [0x20u8; 32];
        assert!(check_target(hash, target));

        hash[0] = 0x30;
        assert!(!check_target(hash, target));

        target = hash;
        assert!(check_target(hash, target));
    }

    #[test]
    fn difficulty_zero_is_zero_target() {
        assert_eq!(difficulty_to_target(0), [0u8; 32]);
    }

    #[test]
    fn higher_difficulty_means_lower_target() {
        let t1 = difficulty_to_target(1);
        let t2 = difficulty_to_target(2);
        let t4 = difficulty_to_target(4);

        assert!(t1 > t2);
        assert!(t2 > t4);
    }

    #[test]
    fn deterministic_hasher_is_stable() {
        let hasher = DeterministicTestHasher;
        let h1 = hasher.hash(&[1, 2, 3, 4], 42).expect("hash h1");
        let h2 = hasher.hash(&[1, 2, 3, 4], 42).expect("hash h2");
        let h3 = hasher.hash(&[1, 2, 3, 4], 43).expect("hash h3");
        assert_eq!(h1, h2);
        assert_ne!(h1, h3);
    }
}
