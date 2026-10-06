# PR #18 Review by ChatGPT (GPT-5.6 Sol)

Reviewed: 2026-10-06  
PR: #18 `test: cut the full test run from 12 minutes to under one`  
Base: `39a568479a2d0bcec3bf181a3821c42c1411f449`  
Reviewed head: `bc845f1b81724af4fce1ae2daf45e94bea6a5c21`

## Scope

This review covers the whole PR, with particular attention to whether the test-speed
optimizations remove safety guarantees that matter to the live-order state machine.

The PR changes production code only minimally:

- `src/trading/read_control.py` replaces a literal SQLite timeout of 1 second with
  `BUSY_TIMEOUT_SECONDS = 1`

The remaining changes are test infrastructure, test parallelization, shorter waits, shared
subtests and case reduction.

Because this repository's live path relies heavily on durable claims, two-store commit ordering,
recovery after process death and last-moment validation before HTTP, test equivalence cannot be
judged only by branch or line coverage. Two tests that execute the same source branch may still
cover materially different safety contracts when they change state at different times.

## CI status

At reviewed head `bc845f1`:

- CI: **success**
- pytest: **2663 passed, 13 skipped, 65 subtests passed**
- pytest duration on GitHub Actions: **87.39 seconds**
- lint: **success**
- secrets: **success**
- CodeQL: **success**

The PR description's local count of 2,676 tests corresponds to 2,663 passed + 13 skipped on the
Linux CI run, plus 65 subtests.

## Positive assessment

Several changes are reasonable and preserve the intended test contract:

- `pytest-xdist` is pinned in the dev dependency range and the lock file is updated.
- Full directory runs use parallel workers while named files/tests stay single-process.
- `-n0` provides an explicit single-process escape hatch.
- Read-only or input-only rejection cases are converted to subtests while retaining every input.
- Busy-lock tests explicitly override timeout values rather than changing production timing.
- The production read-control timeout is only refactored into a constant with the same value.
- Several large Cartesian products are reduced where one common implementation genuinely serves
  every store/stage pair.
- Child-process tests do not inherit pytest's SQLite monkeypatch, so those processes still exercise
  normal SQLite durability settings.

The test-suite speedup is substantial and useful. The issue is not the general optimization
strategy, but a subset of removed cases that represent distinct temporal safety boundaries.

## Test infrastructure note: SQLite durability

The new autouse fixture changes ordinary in-process SQLite connections to:

- `PRAGMA synchronous=OFF`
- `PRAGMA journal_mode=MEMORY` when the database is in DELETE mode

The PR explicitly limits the test contract to process crashes rather than power-loss durability,
and actual process-exit tests run in child Python processes that do not load pytest fixtures.

That can be a reasonable split, but it makes those real child-process crash tests particularly
important. They are now the main tests exercising production-style rollback journal behavior at
the critical live/POST commit boundaries.

For that reason, removing distinct child-process crash phases is more concerning than trimming an
ordinary value matrix.

---

## Findings

### C1. HIGH — Distinct real-process crash boundaries were removed from order dispatch

**File:** `tests/test_private_order.py:785`

The process-exit test previously covered four separate boundaries:

1. `claim` — durable live/POST claim has been recorded, but no HTTP request was sent
2. `response` — HTTP occurred, but no live receipt was saved
3. `receipt` — live receipt was saved
4. `post_completion` — POST control was writing its completion record

The PR keeps only `response` and `receipt`.

The child script still contains code for `claim` and `post_completion`, so those branches are
now dead. This is useful evidence that the reduction removed test scenarios rather than obsolete
implementation code.

The omitted cases are not equivalent to the retained ones:

- `claim` proves that a process death before HTTP does not cause the durable claim to be reused or
  silently resent.
- `post_completion` exercises the other database after the live receipt has already committed,
  which is the cross-store completion boundary.

This is especially important now that ordinary pytest-process SQLite writes deliberately do not
use production durability settings.

**Recommendation**

Restore all four durable process-exit boundaries. If these tests are too expensive for the fast
default suite, move them to a dedicated `durability` marker and run that marker in a separate CI
job rather than deleting the boundaries.

The same class of reduction also appears in `tests/test_private_cancel.py`. In addition,
`test_order_restart.py` and `test_order_resolution.py` removed distinct prepared/completion
process-exit phases while retaining now-dead child-script branches.

---

### C2. MEDIUM-HIGH — Live restart no longer tests GET becoming blocked at the final commit fence

**File:** `tests/test_order_restart.py:367`

Production `LiveOrderJournal.restart()` performs a final validation after the POST-side restart
work and explicitly rejects:

```python
self.posts.reads.status()["blocked"]
```

The old test had one case each for:

- implementation code change
- approval expiry
- read control becoming stopped

The PR removes `read_stop`, leaving only `code` and `expiry`.

The test body still contains:

```python
else:
    reads.stop()
```

which is now unreachable.

This final GET-blocked check is an independent predicate, not another value sent through the same
condition. If that predicate were removed from production, the remaining test cases would still
pass.

The same reduction appears later in
`test_failure_after_post_commit_keeps_live_stopped_with_persistent_preparation`.

**Recommendation**

Restore `read_stop` in both final restart validation tests. It is appropriate to reduce
combinatorial permutations, but the final commit barrier should retain one case for each
independent safety predicate.

---

### C3. MEDIUM-HIGH — Order resolution no longer tests account evidence becoming stale at the final commit boundary

**File:** `tests/test_order_resolution.py:387`

Production order-resolution `validate_commit()` independently checks:

- approval is still within its validity window
- implementation SHA has not changed
- saved account snapshot is fresh
- saved quote is fresh
- read control is not blocked

The old parameter set was:

```
code, expiry, stale, read_stop
```

The PR removes `stale`.

The remaining test body still has:

```python
clock.advance(30 if change == "expiry" else 61)
```

so the 61-second stale-evidence branch is now unreachable.

Approval expiry and account/quote freshness are semantically different. An approval may remain
valid while the observation it authorized has aged past the account policy threshold. The final
freshness check exists specifically to reject that race.

**Recommendation**

Restore the `stale` case. This is a direct test of the account-evidence freshness predicate at
the exact POST-resolution commit boundary.

---

### C4. MEDIUM — Active-cancel claim resolution lost the future-approval lower-bound test

**File:** `tests/test_active_cancel_resolution.py:381`

The old test covered a future-dated approval. The PR removes that parameter but leaves the code:

```python
else:
    accepted = accepted.model_copy(
        update={"accepted_at": clock.now + timedelta(seconds=1)}
    )
```

unreachable.

Expiry tests only the upper half of the consumer's temporal condition:

```
now < expires_at
```

A future approval tests the separate lower bound:

```
accepted_at <= now
```

Active-cancel resolution is a privileged action that clears a local POST claim while deliberately
leaving the broker cancel outcome unknown. The consuming journal should continue to have direct
regression coverage that it refuses an approval that is not valid yet, regardless of whether the
normal builder would create such a value.

**Recommendation**

Restore the future-approval case and remove the dead branch only if the production lower-bound
check is intentionally dropped, which is not recommended.

---

### C5. MEDIUM-HIGH — Restricted cancel no longer tests a journal mutation after the claim but before HTTP

**File:** `tests/test_cancel_authorization.py:218`

The pre-claim test still covers `halt`, but the removed case in this test occurs at a different
time.

This test wraps `begin_cancel()`, so its mutation occurs after:

- `CANCEL_CLAIMED` is appended
- the POST operation is in flight

but before the final HTTP send.

Production then calls `validate_cancel_dispatch(... claimed=True)`, which has claimed-state
checkpoint and event/revision checks in addition to approval expiry and implementation SHA checks.

The PR leaves only `expiry` and `code`. Those do not exercise the event-head / journal-mutation
fence. A regression that stopped checking the claimed journal checkpoint could therefore escape
this suite while both remaining cases continued to pass.

**Recommendation**

Restore the post-claim `halt` mutation case. This is a temporal race test, not a redundant value
variant.

---

## Existing CodeRabbit findings

These were reviewed and appear valid. They are not duplicated as ChatGPT inline comments.

### E1. Minor — fixed 0.5-second child hold may be flaky under xdist load

**File:** `tests/test_read_control.py`

The parent depends on scheduling quickly enough after a child marker appears. Synchronization is
preferable to an arbitrary short sleep.

### E2. Active-cancel `before_post` process-exit boundary was removed

**File:** `tests/test_active_cancel_resolution.py`

The script and expected-result logic still contain the branch, but the parameter no longer reaches
it. This is another distinct cross-store durability boundary.

### E3. Order recovery lost quantity and price mismatch cases

**File:** `tests/test_order_recovery.py`

Client ID, side, quantity and price are independent order identity fields. Keeping only client and
side cannot detect regressions in quantity/price validation.

### E4. Order discovery lost the price mismatch case

**File:** `tests/test_order_discovery.py`

Price is independently significant to order identity and should have a direct test.

### E5. Stream owner wait uses fixed timing slack

**File:** `tests/test_stream_owner_wait.py`

A synchronization primitive is more reliable than assuming the worker remains scheduled for a
fixed `short_owner_wait + 0.3` interval under parallel load.

---

## Other reductions reviewed

I also reviewed the broader reductions and did not treat all removed matrix entries as defects.

Examples that are reasonably defensible include:

- reducing duplicate HTTP status codes when they all intentionally map through one generic
  non-200 path
- rotating bound storage directories across dispatch stages where the exact same inventory
  function is called at every boundary
- reducing repeated long-order loops used to show no quadratic replay
- sharing genuinely read-only validation cases through subtests
- replacing the literal read-control SQLite timeout with the same-valued constant

The important distinction is:

> reduce value combinations when the production invariant is shared, but retain at least one
> direct case for every independent state field, safety predicate and durable timing boundary.

The current PR does not consistently maintain that distinction.

## Merge assessment

The performance improvement is worthwhile, and I did not find a production behavior regression in
the one production module changed by this PR.

However, I do **not** recommend merging the current test reduction unchanged.

Before merge, at minimum:

1. restore the missing real-process durable boundaries described in C1 and CodeRabbit E2
2. restore the independent last-commit safety predicates in C2, C3 and C5
3. restore the future-approval consumer boundary in C4
4. restore independent order-identity fields called out by CodeRabbit E3/E4
5. remove fixed short timing assumptions by using synchronization where practical

There is no need to return to the original 12-minute default run. A better split is:

- fast parallel default suite for ordinary deterministic cases
- a small durability/concurrency suite containing real process death and timing-boundary tests
- run both in CI, while developers can run the fast suite interactively

That preserves the approximately one-minute feedback loop without trading away the tests that
protect against duplicate live orders and unsafe recovery.

---

*Review by ChatGPT (GPT-5.6 Sol)*
