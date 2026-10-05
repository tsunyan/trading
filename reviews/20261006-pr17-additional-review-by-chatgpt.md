# PR #17 additional-commit review by ChatGPT (GPT-5.6 Sol)

Reviewed: 2026-10-06  
PR: #17 `feat: resolve remaining live-operation gaps without a real account`  
Additional commit range: `d9b23ecc3843575829dae1fd3ebf088b87fcaecf..b28c9e51e24a75813596e4891de4caea9fd1932e`  
Reviewed head: `b28c9e51e24a75813596e4891de4caea9fd1932e`

## Scope

This review focused on the ten commits added after `d9b23ec`:

- storage-capacity gates on the live send path
- registered-store capacity coverage
- active-cancel unknown-outcome claim resolution
- stopped/idle legacy GET owner migration
- retained-history benchmarks
- GET-control offline evidence capture
- stream-control offline evidence capture
- registered watchdog / dispatch profiling
- complete archive verification optimization
- evidence metadata validation hardening

The existing CodeRabbit review for the same range was read first so duplicate comments would not be posted.

## CI status

At the reviewed head:

- CI: **success**
- pytest: **2874 passed, 15 skipped, 2 warnings**
- lint: **success**
- secrets: **success**
- CodeQL: **success**

The PR description also records a Windows run of 2889 passing tests. The GitHub connector can verify the GitHub Actions result above; the local Windows result is taken as repository documentation rather than independently reproduced here.

## Existing peer-review findings not duplicated

CodeRabbit currently has three inline findings plus one documentation consistency note for this additional range.

The most important is:

- **Major:** a `STOP_*` audit event appended after `OWNER_UPGRADE_STARTED` makes the current `complete()` / `_install()` checkpoint test fail permanently. Because `stop` remains intentionally callable while the migration is incomplete, the migration can enter a state that the normal completion path cannot finish.

Other existing findings:

- validate non-string/non-null audit `token` values in `read_control_evidence.py`
- clarify swap-accounting wording in `docs/architecture.md`
- update `docs/read-recovery.md` so the remaining-limitations section agrees with the new version 1/2 migration path

Those findings are valid and should be handled, but are not counted again below.

---

## Additional findings

### A1. HIGH — Merely opening `read_owner_upgrade status` can mutate a genuine legacy database before operator approval

**File:** `src/trading/read_owner_upgrade.py:33`

`ReadOwnerUpgrade.__init__()` immediately constructs:

```python
self.reads = PersistentReadLimiter(directory, scope, **clocks)
```

The current `PersistentReadLimiter` constructor opens the source database in a write transaction and performs:

```sql
CREATE INDEX IF NOT EXISTS events_kind_id ON events(kind,id)
```

That index was added later in commit `5085293` specifically as an additive optimization for older databases. A real version 1/2 database created before that change may therefore lack it.

As a result, the documented first inspection command:

```powershell
uv run python -m trading.read_owner_upgrade status ...
```

can change the legacy database before the operator runs `prepare` or `approve`.

The mutation can change:

- database bytes / SHA-256
- schema
- mtime
- SQLite page layout

without creating an owner-upgrade audit event.

This is particularly undesirable because `recovery-evidence-contract.md` explicitly asks operators to preserve the original storage and retain its SHA-256 as investigation evidence.

The current migration tests do not model this condition faithfully. `tests/test_read_owner_upgrade.py::legacy` creates a database with the **current** `PersistentReadLimiter.create()`, which opens it through the current constructor and thus already creates the index. The test then downgrades `version` and removes `owner_file`. It never exercises a true pre-index legacy database.

**Recommended fix**

Use a non-mutating legacy inspector for `status` / proposal preparation, or add an explicit open mode that suppresses additive schema maintenance until the operator-authorized migration transaction.

Add a regression test that removes/omits `events_kind_id`, snapshots the raw database bytes/hash, runs `status`, and verifies exact byte identity.

---

### A2. MEDIUM — Offline evidence artifacts are written directly to the final path without durable atomic publication

**Files:**

- `src/trading/read_control_evidence.py:304-305`
- `src/trading/stream_control_evidence.py:120-121`

The source-copy logic is deliberately careful, but the final artifact is published as:

```python
with args.output.open("x", encoding="utf-8", newline="\n") as output:
    output.write(body)
```

There is no temporary file, `flush()`, `fsync()`, or exclusive final install.

If the process is terminated, the machine loses power, or the filesystem reports an error during the write, a truncated JSON document can remain at the final evidence path. Because subsequent captures use `"x"`, the same official output path then refuses a retry.

This does not grant any recovery permission—the evidence is intentionally diagnostic—but incident evidence benefits from the invariant that any visible final artifact was fully written.

**Recommended fix**

Use the same durable publication pattern already used for immutable acceptance evidence:

1. create a temporary file in the target directory
2. write complete contents
3. flush + fsync
4. install with exclusive no-replace semantics
5. remove the temporary file

The GET and stream evidence writers should share the helper.

---

### A3. LOW-MEDIUM — The new low-disk reason is lost from persistent NotSent audit records at the final pre-HTTP gate

**File:** `src/trading/storage_capacity.py:19`

A low-capacity check raises a useful diagnostic such as:

```
disk_space_low:512MiB
```

`live_doctor` preserves that string and can report the affected gate.

However, when capacity becomes low only at the final dispatch validation—after the POST claim has been entered but before the HTTP client receives the request—the error passes through `private_order._reason()`.

That helper accepts only fixed lower-case reason codes matching:

```
[a-z][a-z0-9_]{0,63}
```

so `disk_space_low:512MiB` becomes the generic:

```
dispatch_refused
```

The durable `SUBMISSION_NOT_SENT` / `CANCEL_NOT_SENT` record then loses the fact that disk capacity caused the refusal. If free space recovers before an operator inspects the system, the doctor no longer reproduces the historical cause.

Safety is preserved; this is an observability / auditability loss.

**Recommended fix**

Separate fixed durable reason codes from dynamic diagnostics. For example:

- durable reason: `disk_space_low`
- optional diagnostic detail: free MiB

or explicitly map `StorageCapacityError` to a fixed safe reason in `_reason()`.

---

## Design note — active-cancel claim resolution deliberately sacrifices retry liveness

The new active-cancel resolution path is internally consistent:

- it does not claim that the original cancellation succeeded or failed
- it keeps POST and live execution stopped
- it requires fresh complete order/account evidence
- it preserves the consumed cancel claim
- it permanently refuses another cancellation attempt for that same local order

This is an intentional at-most-once policy, not treated as a correctness bug in this review.

However, if the original cancellation truly never reached the broker and the order remains active for an extended period, the application itself has no future cancellation route for that order. The remaining path must therefore be operational rather than automatic—for example a separately controlled broker UI/manual action followed by full GET/account reconciliation.

Before real-account acceptance, the operational runbook should explicitly state:

1. who is allowed to terminate such a still-active broker order
2. how that external action is recorded
3. how the local journal is reconciled afterwards
4. how restart is authorized without reinterpreting the original unknown cancel as known

The current documentation clearly states permanent no-repeat semantics, but the operator action for a genuinely persistent live order should be equally explicit.

---

## Assessment

The additional commits materially improve the safety envelope.

Positive points confirmed in code/tests include:

- capacity checks at preflight, after pacing wait, and immediately before HTTP
- capacity coverage for separately mounted registered sync/journal/control/catalog/monitor/cash stores
- doctor uses the same registered-store capacity inventory
- active-cancel resolution keeps the normal account gate untouched during special observation
- active-cancel resolution is cross-bound to POST resolution history and survives the two-database commit boundary
- delayed cancel / later fills remain reconcilable after active-cancel claim resolution
- read/stream evidence capture opens only a byte-for-byte temporary copy with SQLite
- source identity, full bytes and rollback sidecars are checked before/after capture
- owner files are sampled rather than acquired/recreated
- legacy-owner upgrade preserves stop state and refuses unsupported unfinished claims
- archive verification optimization retains the integrity checks covered by the performance comparison suite

### Merge recommendation

Do **not** merge yet while the existing CodeRabbit Major on `read_owner_upgrade` is unresolved.

In addition, A1 should be fixed before treating the legacy-owner migration as safe for real historical control files, because the documented inspection command currently changes the evidence it is meant to inspect.

A2 and A3 are lower-priority hardening/operability fixes but are worth addressing in the same PR while these evidence/capacity paths are new.

---

*Additional-commit review by ChatGPT (GPT-5.6 Sol)*


### A4. MEDIUM — Swap diagnostic can false-positive when the account collection straddles the 06:00 rollover

**File:** `src/trading/swap_check.py:30`

The diagnostic uses the earliest response in the whole account collection:

```python
observed = pd.Timestamp(min(o.response_at for o in report.observations))
```

But the returned positions are taken from the second account sweep, while `observations` starts
with the first sweep's assets request.

A valid collection can therefore cross the rollover boundary like this:

1. first assets response at 05:59:59
2. positions request after 06:00, with the new swap already included in `totalSwap`
3. second sweep also after 06:00, returning the same position state
4. the two-sweep equality check succeeds
5. `swap_check()` nevertheless evaluates expected carry only through 05:59:59

The broker value includes the 06:00 credit while the local expected value excludes it, creating a
false diagnostic mismatch exactly at the boundary being investigated.

**Recommended fix**

Use an observation timestamp tied to the accepted positions snapshot, preferably the second
sweep's positions completion time. If the current flat observation list cannot reliably express
that relationship, expose an explicit `positions_observed_at` or sweep metadata from
`AccountReader`.

---

## Review synchronization note

The PR now contains ChatGPT review comments for A1-A4. Existing CodeRabbit findings listed above
were intentionally not duplicated as inline ChatGPT comments.

GitHub review IDs created during this pass:

- `5417387891` — evidence durability + swap rollover findings
- `5417409204` — legacy DB mutation + durable low-disk reason findings

The current merge recommendation remains unchanged: do not merge while the existing
`read_owner_upgrade` STOP-event Major is unresolved; A1 should also be addressed before using the
migration against genuine historical version 1/2 stores.
