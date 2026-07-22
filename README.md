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
