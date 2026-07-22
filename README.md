# Job Scheduling Simulation

A deterministic Python simulation for scheduled-job visibility when an external scheduler polls once per minute and retrieves at most `X` jobs per poll.

The model keeps submission acknowledgement, scheduler retrieval, execution start, and terminal outcome as separate observed facts. It also measures polling delay, batch backlog, worker delay, EDR freshness, retries, and lifecycle inconsistencies.

## Run

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/pytest
.venv/bin/job-visibility-sim --pretty --output simulation-results/full.json
```

Run a single scenario or the CI subset:

```bash
.venv/bin/job-visibility-sim POLL-04 --pretty
.venv/bin/job-visibility-sim ci --output simulation-results/ci.json
```

Start the visibility API:

```bash
.venv/bin/uvicorn job_visibility.api:app --reload
```

The specification and implementation plan are in [`specs/001-scheduled-job-visibility`](specs/001-scheduled-job-visibility/).

## Architecture assessment

The following assessment uses the four 1–10 criteria from section 7.7 of
[*Serverless Architectures on AWS*](https://learning.oreilly.com/library/view/serverless-architectures-on/9781617295423/OEBPS/Text/ch07.htm#sigil_toc_id_109):
precision, scalability by number of open tasks, hotspot scalability, and cost. Higher
scores are better.

These scores apply to the current in-memory simulation, not the stronger PostgreSQL
architecture proposed in the implementation plan. The scalability scores are provisional
until they are supported by load tests.

| Criterion | Score | Assessment |
| --- | ---: | --- |
| Precision | **5/10** | One-minute polling introduces 0–60 seconds of normal delay. The `X` batch limit and worker capacity can add further polling intervals during a backlog. |
| Scalability—open tasks | **3/10** | Jobs are held in memory, and eligible items are scanned and sorted by a single process. The current implementation is a simulator rather than durable production infrastructure. |
| Scalability—hotspots | **4/10** | Bounded batches protect the scheduler from immediate overload, but excess jobs become backlog. The global lock and single scheduler queue remain bottlenecks. |
| Cost | **9/10** | The simulator is inexpensive to run because it requires only a Python process. Some of this advantage comes from not yet providing production durability and availability. |
| **Total** | **21/40** | **5.25/10 average** |

For context, the chapter's unweighted scores are:

| Solution | Total |
| --- | ---: |
| Cron job | 25/40 |
| DynamoDB TTL | 27/40 |
| Step Functions | 23/40 |
| SQS | 24/40 |
| SQS + DynamoDB TTL | 32/40 |
| **Current simulator scheduler** | **21/40** |

### Precision and polling backlog

If `X=100` and 401 jobs are already eligible, the last batch cannot be retrieved until
approximately the fifth poll:

```text
polls required = ceil(401 / 100) = 5
```

With one-minute polling, some jobs therefore wait about four additional minutes even
when the scheduler operates exactly as designed. The visibility projection explains this
delay, but visibility alone cannot improve execution precision.

### Production target

Completing the persistence and horizontal-scaling work in the implementation plan should
make the following scores realistic:

| Criterion | Target |
| --- | ---: |
| Precision | **6–7** |
| Scalability—open tasks | **8** |
| Scalability—hotspots | **7–8** |
| Cost | **7** |
| **Total** | **28–30/40** |

Reaching that target requires:

- PostgreSQL persistence with an index on eligibility and schedule time.
- Atomic job claims using `FOR UPDATE SKIP LOCKED`.
- Multiple concurrent pollers.
- Queue partitioning or sharding for large hotspots.
- Separate worker queues and autoscaling.
- Backpressure and backlog-age metrics.
- Load tests with hundreds of thousands or millions of open jobs.
- A shorter polling interval or event/timer-based wake-up when sub-minute precision matters.

Visibility and explainability are the current implementation's strongest capabilities. It
distinguishes acknowledgement, retrieval, polling backlog, worker delay, missing EDRs,
retries, conflicts, and stale data. It observes and explains scheduler behavior; it does
not yet provide production-grade scheduling scalability.
