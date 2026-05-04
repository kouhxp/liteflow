# liteflow

A ~1000-line C program that runs YAML-defined DAGs, where an LLM can edit the graph mid-run.

When a task fails, a planner LLM gets the stderr and emits one of four verbs: `RETRY`, `PATCH`, `INSERT_BEFORE`, `ABORT`. The runtime applies the mutation and continues. Every edit is recorded in an append-only event log, so you can replay any run and see exactly which graph changes the LLM made and why.

Not an agent. A runtime where the LLM is a peer of the scheduler.

## Build

```sh
cc -std=c11 -O2 -o liteflow liteflow.c
```

Single file, no libraries. Shells out to `curl` for HTTP. Linux, macOS, modern Windows.

## Run

```sh
export OPENAI_API_KEY=...
export OPENAI_BASE_URL=https://api.deepinfra.com/v1/openai   # any OpenAI-compatible endpoint

./liteflow run examples/demo.yaml
./liteflow replay logs/<run-dir>/events.jsonl
```

## What a workflow looks like

```yaml
name: release
tasks:
  - id: tests
    type: shell
    cmd: "pytest"

  - id: gate
    type: decision
    prompt: "Tests passed. Ship or hold?"
    branches: [ship, hold]
    depends_on: [tests]

  - id: ship
    type: shell
    cmd: "./deploy.sh"
    depends_on: [gate]
    when: gate                    # only runs if `gate` chose this id

  - id: archive
    type: shell
    cmd: "echo done > /var/log/release/last.log"
    on_failure:
      planner: gpt-4o-mini
      budget: 3                   # max LLM mutations applied to this task
    depends_on: [ship]
```

If `archive` fails because `/var/log/release/` doesn't exist, the planner can `INSERT_BEFORE` a `mkdir` task. The graph grows by one node, the original retries, the run finishes green.

## Task types

`shell`, `llm`, `file_read`, `file_write`, `decision`.

`decision` nodes constrain the LLM to picking one of the declared branches. Downstream tasks gate on the choice via `when:`. The unchosen branch ends in a `gated_out` state that doesn't poison its dependents.

## Mutation grammar

| Verb | Effect |
|---|---|
| `RETRY` | re-run unchanged |
| `PATCH` | modify one field (`cmd`/`path`/`content`/`prompt`/`model`), retry |
| `INSERT_BEFORE` | inject a shell remediation task, then retry |
| `ABORT` | give up |

Anything else from the planner is logged and treated as `ABORT`. Per-task budgets cap the number of mutations.

## Audit log

Every state change appends to `events.jsonl`:

```
run_started
task_started        task=archive origin=yaml
task_retry          task=archive rc=1
planner_invoked     task=archive budget=3
mutation_applied    verb=INSERT_BEFORE new_task=mkdir_dir cmd="mkdir -p ..."
task_started        task=mkdir_dir origin=planner
task_succeeded      task=mkdir_dir
task_started        task=archive origin=yaml
task_succeeded      task=archive
run_finished        succeeded=3 failed=0 skipped=0 gated_out=0
```

Tasks the planner created carry `origin=planner` along with the mutation id and parent task. You can always answer *why is this task in the graph?*

## Limits

- YAML parser is a 2-space-indent subset; no block scalars (`|`, `>`), anchors, or multi-doc.
- Tasks share state via files on disk, not templating.
- Run-once CLI; no daemon, no scheduler.
- Open-ended graph synthesis is deliberately not in v1.
