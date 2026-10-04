# PR #15 Review by ChatGPT (GPT-5.6 Sol)

Reviewed: 2026-10-04  
PR: #15 `feat(fx): add practical USD/JPY operations and guarded live execution`  
Reviewed head: `7d331fcd522d61275cf50591c48de6db0d974b4e`  
Base: `main@351f5517722b9f04d0070785691b2c538fab32d5`

## Scope

This PR is unusually large: 147 changed files, 123 commits, +35,227 / -187 lines at the reviewed head.

The review focused on correctness and fail-closed behavior across:

- guarded live execution
- account/order reconciliation
- unknown-result recovery
- live acceptance evidence
- promotion / forward-OOS gating
- public quote/rules acquisition
- scheduled Windows operation
- notification delivery
- audit / dispatch / cycle logs
- backup / segmented journals
- long-running monitoring
- CI and test portability

CodeRabbit reported success as a status, but its actual review was skipped because the PR exceeds its 100-file review limit. Existing Claude handoff notes were treated as known context rather than counted again unless the current code still exposes an independent issue.

## CI status at review time

At `7d331fc`:

- CodeQL: success
- secrets: success
- lint: success
- tests: **failure**
- pytest result: **8 failed, 2478 passed, 7 skipped, 2 warnings**

The current GitHub CI result therefore does not match the handoff note saying that all 2493 tests passed locally.

## Severity

- **BLOCKER**: merge should be blocked until corrected.
- **HIGH**: can invalidate a live-safety, evidence, promotion, or reconciliation guarantee.
- **MEDIUM-HIGH**: significant operational or correctness risk.
- **MEDIUM**: concrete correctness, auditability, or long-running operation issue.
- **LOW-MEDIUM**: worthwhile hardening or scalability fix.

---

## Findings

### 1. BLOCKER — GitHub CI currently fails on eight Windows notification tests

**Files:** `src/trading/windows_notify.py:100`, `tests/test_live_monitor.py`, `tests/test_private_operations.py`

`send_toast()` fails immediately on non-Windows hosts:

```python
if os.name != "nt":
    raise OSError("Windows desktop notifications require Windows")
```

The Linux GitHub Actions job patches `subprocess.run`, but does not bypass this operating-system guard, so the tests never reach the part they intend to verify.

Current CI failures include the parameterized live-monitor notification tests and `test_native_toast_uses_private_fixed_labels_without_exposing_detail`.

**Recommended fix**

Separate toast XML / label construction into a pure platform-independent function and test that function on all platforms. Keep only the native submission boundary Windows-specific. Alternatively, explicitly patch the platform guard in tests, but the pure-function split is cleaner and tests more of the security property directly.

---

### 2. HIGH — `read_acceptance` and `account_baseline` can be forged from arbitrary files

**File:** `src/trading/live_acceptance.py:53,218`

`fingerprint()` accepts every member of `EVIDENCE_KINDS`, including `read_acceptance` and `account_baseline`. The approval CLI then fingerprints every `--evidence kind=path` with the same generic function.

Therefore an operator can create an arbitrary text file and label it:

```
read_acceptance=some-file.txt
account_baseline=some-other-file.txt
```

without ever running `read-evidence` or using a read-only broker credential.

This contradicts the documented acceptance model, where those two evidence kinds are supposed to come from an actual read-only account collection.

The test suite itself demonstrates the bypass by creating ordinary local documents for all evidence kinds in some approval tests.

**Recommended fix**

Make read-generated evidence a distinct typed artifact. Generic `file-evidence` must refuse `read_acceptance` and `account_baseline`; only `read_evidence()` should be able to construct those kinds.

---

### 3. HIGH — Approval creation does not validate read-evidence contents or journal binding

**File:** `src/trading/live_acceptance.py:97-123`

Even if the generic-file bypass above is closed, `approval()` only consumes `AcceptanceEvidence(kind, reference, sha256)`. It never opens and validates the read-evidence JSON.

As a result, it does not verify that a `read_acceptance` / `account_baseline` artifact contains:

- the expected `account_id`
- the current `configuration_sha256`
- a structurally valid read report
- the expected evidence kind
- the expected account-read schema/version

A hand-written JSON file can therefore be fingerprinted and accepted as long as its declared kind is supplied.

**Recommended fix**

Parse read evidence as a strict Pydantic contract during approval construction and bind it to the current activation context. Keep operator documents such as identity/rules/history as opaque fingerprints, but do not treat machine-generated account evidence as opaque documents.

---

### 4. HIGH — All acceptance evidence kinds may point to the same underlying document

**File:** `src/trading/live_acceptance.py:119-123`

`_evidence()` only verifies that the required set of kinds appears exactly once. It does not require independent references or hashes.

The same byte-identical file can therefore be supplied as all five kinds.

That turns the “five evidence classes” requirement into a labeling requirement rather than independent evidence.

**Recommended fix**

For semantically independent evidence classes, reject duplicate SHA-256 values unless a specific pair is explicitly documented as allowed to be identical. Read evidence should additionally have typed provenance as described above.

---

### 5. HIGH — Read-evidence collection has a journal-context TOCTOU window

**File:** `src/trading/live_acceptance.py:76-96`

`read_evidence()` obtains:

```python
context = journal.activation_context()
```

before the broker collection. It then performs network I/O and finally writes the original context into the evidence file.

If the journal revision/configuration/implementation changes while the GET collection is in flight, the saved evidence still claims the pre-collection binding.

**Recommended fix**

Capture a checkpoint before collection and re-read the activation context immediately before writing the artifact. Refuse if the account/configuration/implementation/revision binding changed during collection.

---

### 6. HIGH — Live promotion ignores the frozen candidate's `code_sha256`

**Files:** `src/trading/promotion.py:254-267`, `src/trading/ledger.py`

The frozen hypothesis stores:

- strategy
- strategy parameters
- config SHA
- **code SHA**
- run parameters

and `forward_oos` classification correctly compares the full frozen spec.

However, `require_live()` only verifies:

```python
spec["config_sha256"] == cfg.fingerprint
spec["strategy"] == cfg.strategy
spec["strategy_parameters"] == cfg.strategy_parameters
```

It does not compare the current strategy implementation against `spec["code_sha256"]`.

Therefore the research candidate may be frozen and promoted under one implementation, then `strategy.py`, sizing logic, or related research code may change while the same config continues to pass the live gate.

**Recommended fix**

Recompute the same research `source_sha256()` used by reproducibility records and require it to match the frozen spec. If the desired live implementation deliberately differs from the research package hash, define and persist an explicit deployment-code identity and make that transformation part of promotion rather than silently ignoring code identity.

---

### 7. HIGH — Forward-OOS criteria are optional for live promotion

**File:** `src/trading/promotion.py:175-186`

When no fixed criteria exist:

```python
prefix = "%"
```

so any latest `advance` decision on a `forward_oos` entry satisfies `_forward_advanced()`.

That means a live promotion can still be based on a manually inspected result with criteria decided after seeing the result, despite the new pre-registration feature.

**Recommended fix**

For new live promotions, require a stored immutable forward-OOS criteria record and a judge-generated passing decision. If legacy ledgers must remain supported, make the bypass an explicit legacy/migration mode rather than the default.

---

### 8. HIGH — Judge-origin is inferred from a mutable free-text decision reason

**File:** `src/trading/promotion.py:175-186`

When fixed criteria exist, the code distinguishes judge-generated decisions by matching:

```sql
d.reason LIKE 'fixed criteria <sha-prefix>:%'
```

A manually inserted/recorded `advance` decision can use the same prefix and be accepted as if it came from `judge()`.

The origin of a safety-critical promotion decision should not be encoded in a human-editable reason string.

**Recommended fix**

Persist structured fields such as:

- `decision_origin = "judge"`
- full `criteria_sha256`
- evaluated report SHA
- judge/version identifier

and query those exact fields when allowing live promotion.

---

### 9. HIGH — Strategy promotion is optional in the live strategy cycle

**File:** `src/trading/live_cycle.py:79-82`

Promotion is checked only when `candidate is not None`:

```python
if candidate is not None and not flatten:
    require_live(...)
```

The CLI permits omitting both `--ledger` and `--hypothesis`. In that case a strategy-driven live cycle can still propose and, with `--prepare`, prepare orders even though no strategy has been promoted to live.

The documentation says these arguments should be supplied before real operation, but the code does not enforce the safety property.

**Recommended fix**

For non-flatten strategy-driven live cycles, require an explicit promoted candidate. Keep emergency/manual flatten independent of promotion.

If unrestricted manual live orders are intentionally supported, keep that as a separate operator-intent command rather than making the strategy cycle silently ungated.

---

### 10. HIGH — `complete-history` becomes a permanent unattended assertion in scheduled cycles

**Files:** `src/trading/live_tasks.py:109`, `src/trading/live_order_sync.py:75`

The task plan permanently embeds all `CYCLE_CONFIRMATIONS`, including `complete-history`.

Every future scheduled order reconciliation therefore automatically turns the collected evidence into:

```python
executions_complete=True
```

based on a confirmation made when the Windows task was installed.

History completeness is a property of a particular retrieval / broker state, not a timeless operator statement.

**Recommended fix**

Derive completeness from an observable retrieval protocol or attach a short-lived operator attestation to the exact retrieval/checkpoint. Do not turn an installation-time flag into a permanent assertion for all future executions.

---

### 11. MEDIUM-HIGH — Other scheduled confirmations are also permanent assertions

**File:** `src/trading/live_tasks.py:109`

The same task arguments permanently carry assertions such as:

- complete-account
- account-identity
- external-writers-paused

The documentation explicitly says these are declarations that must remain true while the task is installed, but the software cannot detect when they cease to be true.

For a safety-sensitive unattended scheduler, a permanent CLI switch is too weak a representation of an expiring external invariant.

**Recommended fix**

Represent non-observable operator assertions as expiring attestations/checkpoints, and make the scheduled job fail closed after expiry until the operator renews them.

---

### 12. HIGH — Unknown-order discovery lacks a post-read recovery checkpoint fence

**File:** `src/trading/order_discovery.py:77-99`

The method obtains a recovery context before network I/O:

```python
context = journal.order_recovery_context(client_id)
```

then performs a full account GET and returns:

```json
"journal_changed": false
```

without re-reading the recovery checkpoint.

If another process resolves, changes, or otherwise advances that order while the GET is running, the discovery output can be stale while claiming the journal did not change.

**Recommended fix**

After collection and before returning broker IDs, recompute the exact recovery context/checkpoint and require it to equal the pre-read checkpoint. Otherwise return/refuse as stale.

---

### 13. MEDIUM-HIGH — Valid strategy configurations can require more history than the live fetch cap

**Files:** `src/trading/live_signal.py:218-221`, `src/trading/config.py:20-21`

`history_days()` caps the request at 30 calendar days:

```python
return min(... + 7, 30)
```

but `slow` and `lookback` have no corresponding maximum that guarantees the required warm-up fits in 30 days.

For example, the existing test considers `slow=1000` valid and returns 30 days, despite roughly 42 days of hourly bars being needed before adding weekends/closures.

The helper is documented/tested as covering warm-up, but it does not for all valid settings.

**Recommended fix**

Either remove the 30-day cap and use bounded chunked GMO requests, or constrain strategy settings such that the cap provably covers every valid warm-up.

---

### 14. MEDIUM — A hold/no-intent cycle leaves the previous `intent.json` valid on disk

**Files:** `src/trading/live_signal.py`, `src/trading/live_cycle.py`

The intent file is written only when a new intent exists. A later cycle that decides `hold` does not delete or replace the old file.

An operator can therefore see the current cycle saying “hold” while an old actionable `intent.json` remains available for a separate prepare command.

**Recommended fix**

On every cycle, atomically replace intent state. For no-intent decisions, remove the stale file or write a typed non-actionable envelope with the cycle/checkpoint that invalidates the previous intent.

---

### 15. MEDIUM — Task-plan numeric validation permits permanently failing scheduled jobs

**File:** `src/trading/live_tasks.py:51-57`

The planner only checks that `max_slippage` and `valuation_tolerance` are finite.

Values such as:

- negative/zero slippage
- nonsensical valuation tolerance
- fixed units outside the configured/broker lot constraints

can therefore be installed as an hourly task and fail on every execution.

**Recommended fix**

Validate all static arguments at plan/install time using the same domain constraints as the execution path. A scheduled task should not be installable when its immutable inputs can already be proven invalid.

---

### 16. MEDIUM — Live auto-sizing converts exact monetary values to float

**File:** `src/trading/live_signal.py:181`

Auto sizing does:

```python
entry_units(float(account.balance), float(account.equity), float(quote.ask), cfg)
```

The live account and quote models otherwise deliberately preserve Decimal precision. Converting to binary float can change a result at a lot-size boundary and makes live sizing subtly diverge from exact accounting.

**Recommended fix**

Provide a Decimal-based sizing implementation for the live path, ideally sharing a single exact arithmetic routine with research/paper where practical.

---

### 17. MEDIUM — Proposal notification deduplication ignores quantity, price and exact holdings

**File:** `src/trading/live_cycle.py:362`

The notification key is:

```python
f"{side}:{effect}:{current}"
```

If a proposal stays BUY/OPEN but changes from 1,000 to 5,000 units, or materially changes its price protection, the operator may receive no new notification.

Those are precisely the changes a human reviewer should be made aware of.

**Recommended fix**

Use a digest of the full normalized intent plus the relevant state checkpoint as the notification identity.

---

### 18. MEDIUM — Result file is persisted before later history/dashboard failures are recorded

**File:** `src/trading/live_cycle.py:372-390`

The cycle writes `result_output` first. Afterwards it may set:

- `history_written = false`
- `dashboard_written = false`

but does not rewrite the result file.

Therefore the result file consumed by the doctor/dashboard can falsely omit the fact that those downstream outputs failed.

**Recommended fix**

Perform non-critical output attempts first, update the final result structure, then atomically write the canonical result file last.

---

### 19. MEDIUM — Cycle history grows forever and dashboard rereads the entire file

**Files:** `src/trading/live_cycle.py:181`, `src/trading/live_dashboard.py:77`

The history is append-only JSONL with no rotation. The dashboard needs only the latest `limit` rows but executes:

```python
Path(path).read_text(...).splitlines()[-limit:]
```

This makes every dashboard update O(total lifetime history) in time and memory.

**Recommended fix**

Use tail-oriented bounded reads, an indexed SQLite table, or explicit history rotation.

---

### 20. MEDIUM — Dispatch history has the same unbounded-read problem

**Files:** `src/trading/order_runtime.py:138-150`, `src/trading/live_report.py:26-37`

The dispatch log grows indefinitely and `read_dispatches()` reads the whole file on every report/dashboard generation.

A long-running account therefore accumulates increasing report latency and memory usage.

**Recommended fix**

Move dispatch evidence into an indexed append-only store or rotate logs while preserving an audit chain.

---

### 21. MEDIUM — JSONL audit writers do not have cross-process locking

**Files:** `src/trading/live_cycle.py:181`, `src/trading/order_runtime.py:147`

Both history and dispatch logs are opened in append mode without an OS/process lock.

Task Scheduler's `IgnoreNew` prevents overlap for the same scheduled task, but does not prevent:

- manual CLI invocation
- doctor/other tools
- a second scheduled task
- another process using the same file

from appending concurrently.

Audit log records can therefore interleave or be corrupted.

**Recommended fix**

Use an OS lock around each append or move the audit stream to SQLite with transactions.

---

### 22. MEDIUM — Dashboard silently hides corrupt history records

**File:** `src/trading/live_dashboard.py:77-81`

Malformed JSON history lines are silently skipped.

For a UI convenience cache this might be acceptable; for an operational/audit history it converts corruption into apparent absence.

**Recommended fix**

Expose a visible `history_corrupt` / skipped-record count, and preferably fail the audit view closed while still allowing a diagnostic page to render.

---

### 23. MEDIUM — Live report silently hides corrupt dispatch records

**File:** `src/trading/live_report.py:26-37`

`read_dispatches()` similarly ignores malformed or structurally invalid records.

This makes “no dispatch measurement exists” indistinguishable from “the dispatch audit log is damaged”.

**Recommended fix**

Return explicit integrity metadata or raise a dedicated report-integrity error. Do not silently reduce `orders_measured` because an audit record became unreadable.

---

### 24. MEDIUM — Slippage measurement trusts a mutable sidecar log rather than journal-bound evidence

**File:** `src/trading/live_report.py:26-37,94-108`

Dispatch records contain a checkpoint SHA, but `read_dispatches()` discards it and maps only `client_id -> quote`.

The live report then computes execution slippage from that mutable JSONL quote.

Editing the file can therefore change the reported execution-cost metric without changing the journal.

**Recommended fix**

Bind the dispatch record to the exact prepared/submitted journal checkpoint and verify the hash before using the quote. Better yet, persist reviewed dispatch quote evidence in the append-only journal itself.

---

### 25. MEDIUM — Live DB backup no-overwrite check has a TOCTOU race

**File:** `src/trading/live_setup.py:71-83`

The code checks:

```python
if output.exists():
    ...
```

then later opens `sqlite3.connect(output)`.

If another process creates the file between those operations, SQLite opens that existing DB and `source.backup(target)` can replace its contents despite the intended “never overwrite” semantics.

**Recommended fix**

Backup into a uniquely created temporary path, fsync/verify it, and install it with an exclusive no-replace operation.

---

### 26. LOW-MEDIUM — Backup SHA calculation reads the entire database into RAM

**File:** `src/trading/live_setup.py:88`

```python
hashlib.sha256(output.read_bytes()).hexdigest()
```

loads the complete database in memory. The live events DB can grow continuously.

**Recommended fix**

Hash the file incrementally in fixed-size chunks.

---

### 27. MEDIUM — Segmented journal still performs whole-history archive reads during normal reopen paths

**Files:** `src/trading/segmented_journal.py:125,359`, `src/trading/private_operations.py:582`

Segmentation solves the active-journal capacity issue, but `SegmentedEventJournal.__init__()` invokes `check_history()`, which verifies sealed archives.

The private watchdog repeatedly constructs `PrivateSyncWorkspace`, which constructs the segmented journal. Long-running monitoring therefore trends back toward O(total archived history) work.

**Recommended fix**

Separate:

- cheap active/head/anchor validation required for normal operation
- explicit full historical audit

and only perform full archive verification on an audit command or bounded cadence.

---

### 28. MEDIUM — Forward criteria allow NaN and Infinity

**File:** `src/trading/promotion.py:69-83`

Criteria values are checked only as `int | float` and not for finiteness.

Python/JSON tooling can therefore admit non-finite values such as NaN or Infinity. These produce meaningless or trivially passing/failing comparisons.

**Recommended fix**

Require a finite Decimal-compatible number and serialize with strict JSON that rejects NaN/Infinity.

---

### 29. LOW-MEDIUM — Promotion judge reads report artifacts with no size bound

**File:** `src/trading/promotion.py:135`

`report.json` is loaded with `read_bytes()` without a maximum size before hashing/parsing.

A corrupt or unexpectedly huge artifact can make promotion checking consume unbounded memory.

**Recommended fix**

Apply a report-size bound and stream the SHA calculation before bounded JSON parsing.

---

### 30. MEDIUM — `closed_trades` statistics are actually closing-order statistics

**File:** `src/trading/live_report.py:41-55,109-113`

One CLOSE order may settle up to 10 positions, but the report appends one outcome per close order:

```python
outcomes.append(sum(... for e in fills))
```

and labels the result:

- closed_trades.count
- win_rate
- profit_factor

A single close that combines several independent positions is therefore counted as one “trade”.

**Recommended fix**

Either rename the metric to `closing_orders`, or define a real trade identity and compute outcomes per closed position/trade.

---

### 31. LOW-MEDIUM — Public rule evidence is not flushed/fsynced before being treated as immutable evidence

**File:** `src/trading/live_rules.py:92-96`

The rules-evidence writer uses `open("xb")` and writes bytes, but unlike the newer acceptance writers it does not flush/fsync the evidence before returning.

Because this file is later fingerprinted as acceptance evidence, durability policy should be consistent.

**Recommended fix**

Use the same immutable temporary/exclusive-write + flush + fsync pattern used by the acceptance evidence path.

---

## Recommended merge order

The highest-priority fixes are:

1. restore green GitHub CI
2. make read acceptance evidence non-forgeable by generic file labeling
3. validate machine-generated evidence contents and journal binding
4. add a post-collection fence to read evidence and order discovery
5. require frozen code identity at live promotion/use
6. make fixed forward-OOS criteria mandatory and structurally identify judge decisions
7. make strategy promotion mandatory for the strategy-driven live cycle
8. remove permanent `complete-history` assertions from unattended scheduling
9. make valid strategy warm-ups always fetchable

After those are fixed, the logging, backup, notification dedupe, long-run archive, and reporting issues should be addressed before calling the live workflow operationally mature.

## Positive observations

Despite the findings above, the core execution path is substantially more defensive than a typical first live-trading implementation.

In particular, the reviewed code generally does a good job of:

- separating proposal/preparation from actual POST
- maintaining explicit POST claims
- preserving unknown outcomes instead of retrying blindly
- using order checkpoints before credential loading and dispatch
- fail-closing on local-state ambiguity
- reconciling accepted orders before proceeding
- keeping emergency flatten behavior separate from strategy promotion
- binding live credentials to the dedicated journal/control instances
- preserving append-only/restart evidence for critical state transitions

The main remaining risk is not “the HTTP POST code retries recklessly”; it is that some of the *evidence and promotion gates around that POST* are currently easier to satisfy than their names/documentation imply.

---

*Review by ChatGPT (GPT-5.6 Sol)*
