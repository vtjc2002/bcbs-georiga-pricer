# Georgia price checker

A prototype for turning large Blue Cross Blue Shield Transparency in
Coverage disclosures into queryable Parquet and small public lookup files.
The static website uses GitHub's **Primer** design system and is prepared for
**GitHub Pages**. The cloud implementation now targets **Azure Synapse**:
Spark for bounded streaming ingestion, serverless SQL for Parquet transformations,
and Azure Storage for private data and public lookup files. This replaces the
earlier Container Apps execution proposal. Azure rollout remains unperformed.

**Not a personalized insurance quote.** Negotiated prices do not include your
deductible, copay, eligibility, or all charges in an episode of care.

## Coverage

The supplied Premera employer index links seven files labeled
`Georgia, Anthem Blue Cross and Blue Shield` to:

- Your Choice NGF - SF
- Your Future HSA Qualified Agg NGF - SF

This is **not all Georgia BCBS plans** or the complete nationwide disclosure for
these two plans. All providers in the selected files are retained regardless of
their address. Out-of-network allowed amounts are excluded.

The observed seven files total **2,609,230,914 compressed bytes**. The November
index points to October rate files. The website distinguishes the index date
from each rate file's data date.

## Current boundaries

The local container, streaming adapter, Parquet queries, lookup generator, static
UI, Pages workflow, Azure publication helper, and Synapse/storage infrastructure
templates are implemented. **No production data or cloud resources are deployed.**

Current-index discovery uses the official HealthSparq portal's
`latest_metadata.json` feed, verified from inside the Linux container. It selects
the exact Microsoft employer index and follows its published path, then checks
the publisher, linked plans, Georgia description, source count, and source hosts.
It does not guess next month's filename or retain expiring signed rate URLs.
An explicit `-Index` or `INDEX_URL` overrides discovery for reproducible pilots.

The Synapse ingestion adapter, serverless SQL transformation plan, distributed
lookup generation, pipeline/trigger artifact builder, and reference-aware retention
are implemented. Local Spark fixture execution reproduces the local lookup
contract; Blob checkpoint/publication behavior is exercised against Azurite.
**These checks do not prove live Synapse SQL, managed identity, firewall policy,
full-source performance, or subscription cost.** Cloud acceptance is still required.
The full local seven-file acceptance
run was stopped when the cloud architecture changed; completed checkpoints were
preserved. The Bicep template now defines Synapse and an auto-pausing Spark pool.
The current local implementation
retains normalized files on disk and stops at an explicit output/scratch budget;
it must not be presented as an unlimited-disk cloud ETL.

## Synapse ETL

**Serverless SQL is not a drop-in raw JSON ingestion engine for these sources.**
Synapse SQL supports GZIP through the CSV reader with `PARSER_VERSION='1.0'`
and `DATA_COMPRESSION='GZIP'`. However, the documented JSON query pattern reads
each JSON document into a SQL string before applying `OPENJSON`. These sources
contain giant root objects with deeply nested arrays, not independent bounded
JSON records. GZIP support does not make that pattern a bounded, token-level
parser. Parser 2.0's documented 8 MB row limit is not a solution either: that
parser does not support `DATA_COMPRESSION`.

The no-Docker execution path is:

1. Discover the current employer index and validate the same two plans, Georgia
   description, source count, and source hosts. Record immutable source identities
   separately from expiring signed URLs.
2. Execute a Synapse Spark job using a streaming parser for each source and write
   bounded normalized Parquet parts to private ADLS Gen2 storage. Commit a source
   checkpoint only after full gzip EOF/CRC and source validation. Do not retain
   all sources or all normalized output on a worker's local disk.
3. Use serverless SQL over the normalized Parquet for relational checks and
   transformations. Materialize derived tables with CETAS into new immutable
   output prefixes. Keep provider memberships separate from negotiated prices
   instead of expanding a rate across every NPI.
4. Generate the existing bounded, compressed lookup JSON through a separate
   publication step, then conditionally replace `active.json` only after all
   seven selected sources and lookup outputs pass validation. CETAS Parquet
   output is not itself the website's JSON serving contract.
5. Orchestrate monthly discovery and changed-source processing with Synapse
   pipelines. Reuse unchanged completed sources, refresh expired links, and
   retain the previous good public snapshot after any failure. Pipeline charges
   must be included in the eventual cost measurement.

Spark and serverless SQL have different billing and execution models.
Spark instances have a minimum of three nodes; use automatic pause and measured
concurrency rather than assuming a free single-worker job. A normal gzip file
cannot be split into independently readable chunks merely by adding workers.
The current ingestion phase is deliberately sequential on the Spark driver;
allocated workers still incur charges during that phase. Source-file parallelism
is a future measured optimization, not an implemented claim. After conversion,
Parquet is processed in parallel. Do not substitute `wholeTextFiles`, `json.load`, or a
whole-root `spark.read.json(..., multiLine=True)` for bounded streaming ingestion.

`etl\synapse.py` reuses the event parser with a remote Parquet sink. It keeps
bounded row batches and at most one approximately 32 MiB part per table on local
disk, uploads closed parts, and unlinks them. Incomplete attempts cannot become
checkpoints. Strong-validator checkpoints are restored from Blob; changed plan
mappings are taken from the current index rather than the old checkpoint.

The generated SQL checks source-scoped reference integrity and writes rate-to-group
edges with CETAS, never a rate-by-NPI cross product. Nested Parquet arrays are
preserved as JSON strings across the SQL boundary and parsed back to arrays in
Spark; decimal amounts retain `decimal(28,8)` precision.

`etl\synapse_job.py` repartitions and externally sorts code/provider/group data in
Spark. Streaming reducers produce bounded objects through the existing lookup
contract. Intermediate Parquet spool files hold at most 16 objects each and are
read one batch at a time for publication; the driver never collects the provider
membership dataset or an entire Spark result partition.

The pipeline serializes refreshes. The publication lease and conditional active
pointer replacement protect activation. A separate retention activity follows
successful publication: it retains current/previous snapshots and source parts,
protects runs/checkpoints newer than seven days, invalidates obsolete checkpoints
before deleting their data, and removes older unreferenced artifacts. A retention
failure is reported separately after publication; it does not invalidate already
published prices. Old SQL metadata objects are not automatically dropped.
No Azure resource or paid execution has been started.

References:
[SQL GZIP and parser options](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/develop-openrowset),
[SQL JSON reading](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/query-json-files),
[CETAS](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/develop-tables-cetas),
[Spark pool configuration](https://learn.microsoft.com/en-us/azure/synapse-analytics/spark/apache-spark-pool-configurations),
[gzip parallelism](https://learn.microsoft.com/en-us/azure/databricks/optimizations/spark-ui-guide/one-spark-task).

## Local container workflow (Windows + wslc)

These commands remain available for local regression tests and historical
benchmarks. They are no longer the proposed production ETL deployment path.

Run commands in this worktree using PowerShell. A separate Ubuntu distribution,
Docker Desktop, and a Windows Python environment are not required for the ETL.
`wslc.exe` must be installed and its container runtime responsive.

```powershell
.\scripts\Test-Local.ps1 -Mode Test
```

The helper builds `etl\Dockerfile`, runs the Linux container with one CPU and
2 GiB memory, and executes the fixture suite. No Azure resource is created.

If dependency downloads work on Windows but fail inside the build due to a TLS
handshake/network issue, download Linux wheels on the host and use the offline
Dockerfile. This does **not** disable certificate verification:

```powershell
python -m pip download --only-binary=:all: `
  --platform manylinux_2_28_x86_64 --platform manylinux2014_x86_64 `
  --python-version 312 --implementation cp --abi cp312 `
  --dest etl\wheelhouse -r etl\requirements.txt
.\scripts\Test-Local.ps1 -Mode Test -Offline
```

The ignored wheelhouse is local build input, not a committed dependency cache.
The offline and online images install the same pinned requirements. Container
results provide a regression reference; they do not prove Synapse runtime,
identity, or remote-storage behavior.

### Real-file pilots

Keep the current employer index outside Git. Set `$index` to its absolute Windows
path. It is mounted read-only and its signed URLs are never committed.

```powershell
$index = 'C:\path\to\current-employer-index.json'
.\scripts\Test-Local.ps1 -Mode Pilot -Offline -Index $index `
  -Filename '2026-10_102_11B0_in-network-rates_4_of_7.json.gz'
```

Use the actual current filename from the index, not the example month's name.
Run a smaller shard before the largest. A pilot writes private Parquet and a
benchmark under ignored `.local`; it **never publishes an active public snapshot**.

One completed pilot of the example `_4_of_7` file produced these observations:

| Metric | Observed value |
| --- | ---: |
| Compressed input | 95,796,487 bytes |
| Provider membership entries | 23,594,663 |
| Provider organizations | 1,436,072 |
| Provider groups | 5,329 |
| Services | 16,961 |
| Negotiated prices | 390,910 |
| Private Parquet/checkpoint output | 133,349,482 bytes |

These are measurements of **one shard**, not an extrapolation of the full
dataset. Provider memberships are kept separate from prices so they do not
multiply the serving rate table. The first projection experiment exceeded the
512 MB DuckDB limit during global aggregation; the implementation now uses
external sorting and bounded streaming groups rather than that aggregation.

A subsequent complete projection of this shard on a native Linux volume took
**103.358 seconds** under one CPU / 2 GiB, with about **678 MiB peak process RSS**.
It generated 16,961 codes and 4,096 provider buckets. Deterministic gzip reduced
the public objects from **1,318,645,769 bytes to 54,300,062 bytes** (about 96%).
These remain one-shard measurements, not full-dataset acceptance or an active
public dataset.

### Full local run

```powershell
.\scripts\Test-Local.ps1 -Mode Run -Offline
```

Without `-Index`, the run discovers the current employer index from the publisher.
The run selects the configured plans and all seven Georgia sources, validates
them, generates lookup files, and atomically switches `.local\public\active.json`
only after success. A changed source count or hostname requires explicit review
in `metadata\source.json`.

The CLI's default **3 GiB output/scratch ceiling** and DuckDB's **512 MB memory /
2 GB spill ceiling** are safety gates, not a claim that every monthly dataset
fits. The root output budget includes cached private data and public snapshots.
These limits describe the local prototype, not Synapse worker sizing.
Per-write disk accounting is incremental; full directory scans occur only at
source boundaries, avoiding repeated scans of thousands of files on Windows mounts.

For representative Linux filesystem performance, use a dedicated native volume
instead of a Windows bind mount, especially when generating thousands of lookup
objects. The output remains in the named volume rather than `.local`:

```powershell
wslc volume create georgia-pricer-local
.\scripts\Test-Local.ps1 -Mode Run -Offline -Volume georgia-pricer-local
```

Mount that same volume at `/output` in subsequent container commands to inspect
the benchmark or publish the validated snapshot. Do not remove it while a run
is active or before preserving any results you need.

Previously completed shards with the same path and strong ETag/length are reused.
Rotated query-string signatures do not create new source identities. Sources
without strong validators are re-read; gzip is parsed sequentially rather than
resumed at an arbitrary compressed byte offset.

## Query the private data

Each source directory contains typed Parquet tables:

| Table | Contents |
| --- | --- |
| `groups` | Source-scoped provider group IDs and network names |
| `members` | Provider group memberships and NPIs |
| `organizations` | Typed organization identifiers; private only |
| `services` | Text billing codes, versions, descriptions and arrangements |
| `sets`, `set_groups` | Normalized service-to-provider-group relationships |
| `prices` | Decimal amounts, price types, expiration, setting, modifiers and place of service |

Use DuckDB to query these files; no database server is required. Group IDs are
only unique within a source, and an employer EIN is not a unique plan ID.
Descriptions and TINs are not emitted in the public lookups.

The adapter targets observed CMS schema **2.x** provider-reference files.
Unsupported legacy inline/external provider representations, unresolved
references, severity-dependent services whose context is not supported, invalid
prices, incomplete gzip, and oversized public objects fail
explicitly. This adapter is not a substitute for the complete CMS schema
validator or an assertion of clinical usefulness.

## Public lookup contract

Only generated lookup JSON is public. Raw source material, Parquet, operational
state, and signed URLs remain private.

```text
active.json
snapshots/<content-hash>/manifest.json
snapshots/<content-hash>/catalogue/<hash-prefix>.json
snapshots/<content-hash>/codes/<code-hash>/index.json
snapshots/<content-hash>/codes/<code-hash>/<page>.json
snapshots/<content-hash>/providers/<npi-hash-prefix>.json
snapshots/<content-hash>/groups/<group-hash>/index.json
snapshots/<content-hash>/groups/<group-hash>/<page>.json
```

These are HTTP object names. Snapshot files are stored locally as `.json.gz`
and uploaded without the `.gz` suffix, with `Content-Encoding: gzip`.
`active.json` stays small and uncompressed. Compression happens during projection,
not only during upload, so the local disk budget benefits as well.

Searches use exact code system/code values and optional NPI, plan, and network
filters. An NPI-only search browses its disclosed groups and procedure codes.
Prices are paged by code without expanding each price to every NPI. Filters
apply to each bounded source page, so an empty filtered page can have later
matches; the UI explains this and exposes next/previous navigation.

Percentages, per-diem rates, professional/institutional charges, modifiers,
settings, and arrangements are shown separately. No cheapest-episode ranking
is inferred. A missing result does not prove lack of coverage.

## Local storage and browser integration

Test actual Blob leases and conditional publication against an isolated
temporary Azurite container:

```powershell
.\scripts\Test-Local.ps1 -Mode Test -Offline
.\scripts\Test-Storage.ps1
```

The storage helper generates temporary emulator credentials under ignored
`.local`, runs both containers on a task-specific network, and cleans up that
network, emulator, and credentials afterward. Azure managed identity/RBAC and
subscription policies still need a separate cloud smoke check.

For the UI:

```powershell
npm --prefix web ci
npm --prefix web test
npm --prefix web run build
npm --prefix web run dev
```

Load a generated public lookup base URL in the UI, or set `VITE_DATA_BASE_URL`
before building. A missing dataset shows an explicit unconnected/error state,
not mock prices.

For a clearly synthetic, isolated browser fixture:

```powershell
$preview = Join-Path (Get-Location) '.local\preview'
New-Item -ItemType Directory -Force $preview | Out-Null
wslc run --rm --cpus 1 --memory 2G `
  --mount "type=bind,source=$preview,target=/preview" `
  --entrypoint python georgia-pricer:local -m tests.preview /preview
python scripts\serve-preview.py --directory .local\preview\public
```

With the Vite server on `http://127.0.0.1:5173`, load the dataset at
`http://127.0.0.1:8765/`. The synthetic fixture uses code `CPT 00123` and NPI
`1111111111`. Never publish this fixture as real rate data.

## GitHub Pages

`.github\workflows\pages.yml` builds/tests only the frontend and deploys `web\dist`.
After merging the workflow, select **GitHub Actions** as the repository's Pages
source. Set repository variable `PUBLIC_DATA_BASE_URL` to the public Blob
container URL ending in `/`. No Azure credentials are needed in the Pages build.

Relative asset URLs and hash navigation support repository project paths without
server-side rewrites. Monthly data changes are discovered from `active.json`
at load/refresh and do not require rebuilding the UI. A search session pins one
immutable snapshot.
Hash-linked searches execute after the dataset loads, including when a dataset
URL is entered manually.

GitHub Pages hosts **UI assets only**, never the dataset. Its free hosting
limits and prohibition on commercial SaaS still apply. Do not use this site for
member authentication, payments, or personal benefit information.

## Azure foundation and publication

`infra\main.bicep` defines private ADLS Gen2 and public Hot LRS storage, a
Microsoft Entra-only Synapse workspace, a three-node Small Spark 3.5 pool with
five-minute auto-pause, blob-only public lookup access, Pages-origin CORS, and
container-scoped workspace identity grants. There is **no ACR, container image,
Container Apps job, or dedicated SQL pool**.

Compile without deploying:

```powershell
az bicep build --file infra\main.bicep --outfile .local\main.arm.json
```

Choose a region, subscription and resource group, and review public-access
policy/costs before any deployment. Parameters require a region, Pages origin,
Entra administrator name/object ID, SQL bootstrap client IP, and an explicit
`allowAzureServices` network decision. Allowing Azure services opens SQL network
access to other Azure tenants too, but does not bypass Entra/SQL authorization;
without this rule, provide an approved private-network path for pipeline SQL
activity. CORS takes `https://OWNER.github.io`, not the repository path.
Set the intended subscription explicitly with `az account set --subscription ID`
before deploying or publishing.

Build the notebook, deterministic code bundle, serverless linked service,
monthly pipeline, SQL bootstrap script, and **inactive** schedule without Azure:

```powershell
python scripts\build_synapse.py --workspace WORKSPACE `
  --private-account PRIVATEACCOUNT --serving-account SERVINGACCOUNT
```

Generated artifacts are ignored under `.local\synapse`. To publish them to an
already-provisioned workspace, after reviewing permissions:

```powershell
.\scripts\Publish-Synapse.ps1 -Workspace WORKSPACE `
  -PrivateAccount PRIVATEACCOUNT -ServingAccount SERVINGACCOUNT
```

The deployer needs Synapse artifact permissions and Blob Data Contributor on the
private account's `workspace` container. Publishing does **not** create the
database, run ETL, or enable a schedule. Run generated `bootstrap.sql` as the
configured Entra administrator against the workspace's **serverless** SQL
endpoint. It creates the application database, grants the workspace identity
database-local ETL permissions, and sets SQL data-processed safeguards.

Then manually run `GeorgiaMonthly` from Synapse Studio, or:

```powershell
az synapse pipeline create-run --workspace-name WORKSPACE --name GeorgiaMonthly
```

Inspect the run's `Ingest -> IfChanged -> Transform -> Publish -> Retain`
activities. `Transform` is a serverless SQL Script activity, not Spark SQL.
`Publish` cannot run after a failed SQL check or CETAS. Unchanged successful
snapshots skip transformation/publication on subsequent checks in the same index
month. Expired source links trigger a bounded rediscovery attempt. SQL and
publication retries start a new pipeline run, reusing completed source checkpoints;
do not resume a failed Publish activity into its existing immutable spool path.

Before scheduling, complete cloud acceptance: verify RBAC and SQL access, ingest
a complete real source and the largest source with measured CPU/storage/scan
usage, reconcile exact decimal/context values, perform all-seven-file publication,
interrupt a refresh and confirm last-good data, repeat unchanged input, and
exercise the deployed Pages origin against public Blob CORS/gzip.

Only after that acceptance, import and enable the schedule:

```powershell
az synapse trigger create --workspace-name WORKSPACE --name GeorgiaSchedule `
  --file "@.local\synapse\GeorgiaSchedule.json"
az synapse trigger start --workspace-name WORKSPACE --name GeorgiaSchedule
```

It checks on the 3rd, 10th, and 17th of each month at 06:00 UTC to accommodate
late publication. Each check starts Spark, even if all source checkpoints are
reused. That startup cost is intentional and must be measured; it is not free.
The trigger is never enabled automatically by the publishing helper.

The Spark fixture can also run outside Azure on a Linux development runtime
with Python 3.11, Java 17, the ETL test dependencies, and `pyspark==3.5.7`.
Set `RUN_SPARK_TESTS=1` and run `python -m pytest -q -k real_spark`.
It compares every generated lookup object to the local reference, exercises the
Parquet spool and driver publication path, and injects an interrupted upload.
Azure SDK checkpoint/upload integration uses the isolated Azurite helper above;
neither check substitutes for a live serverless SQL/managed-identity smoke run.

The existing Python `publish` command uploads a **fully validated local output** to
existing accounts using `DefaultAzureCredential` (managed identity in Azure).
It acquires a renewable Blob lease, checks immutable-object checksums, and
conditionally replaces the active pointer after all uploads succeed. A failed
upload leaves the previous pointer in place. The executing identity needs Blob
Data Contributor access to the two containers.
Serving objects are deterministically gzip-compressed with the correct HTTP
content encoding to reduce stored bytes and public-download bandwidth; browsers
decompress transparently and still enforce the uncompressed 4 MiB limit. Private
objects use `private, no-store`, while immutable serving objects are publicly
cacheable and `active.json` is revalidated.

Do not pass account keys or signed rate URLs to the static website. The local publisher remains available independently. Remote checkpoint reuse
and reference-aware retention are implemented in the Synapse pipeline, not in
this local `publish` command.

## Cost guidance

The serving design avoids a continuously running database and public SQL scans:

- **GitHub Pages:** free static frontend within its usage limits.
- **Azure Blob Hot LRS:** illustrative East US storage rate USD 0.0208/GiB-month;
  reads, writes and direct public-download egress are additional.
- **Synapse serverless SQL:** charged by data processed, including storage reads,
  intermediate transfers, and CETAS writes; not simply the compressed raw input
  size. Each query has a 10 MB minimum, rounded up to MB. Partitioned Parquet and
  explicit column selection reduce unnecessary processing.
- **Synapse Spark:** separately billed active compute, including the minimum
  cluster allocation and idle time before automatic pause. No free ETL allowance
  is assumed.
- **Synapse pipelines:** orchestration and any data-movement activity add charges.
- **ACR/Container Apps:** not needed and not included in the Synapse template.

The earlier USD 6-15/month container-based envelope is **withdrawn for the Synapse
design**. There is no measured Synapse monthly total yet. It depends on actual
Spark node-time, SQL data processed, pipeline activity, retries, retained output,
traffic, and monitoring. GitHub Pages' bandwidth allowance does not cover Blob
downloads.

Use SQL daily/weekly/monthly data-processed limits as one safeguard, not a total
Azure spending cap: an in-flight query is not terminated when a limit is crossed,
and those limits do not cover Spark, pipelines, storage, or egress. Azure budget
alerts also do not automatically stop spending. Benchmark a small complete source
and then the largest source before enabling monthly automation. Choosing managed
Synapse execution removes the Docker dependency; it does not establish that it is
cheaper than the earlier container design or free local DuckDB.

References: [CMS schema](https://github.com/CMSgov/price-transparency-guide),
[Premera](https://www.premera.com/visitor/transparency),
[Synapse pricing](https://azure.microsoft.com/en-us/pricing/details/synapse-analytics/),
[serverless SQL cost controls](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/data-processed),
[Blob pricing](https://azure.microsoft.com/en-us/pricing/details/storage/blobs/),
[Pages limits](https://docs.github.com/en/pages/getting-started-with-github-pages/github-pages-limits),
[Primer](https://primer.style/product/getting-started/react/).
