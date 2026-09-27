"""Dataset campaign (prompt 15): replay every profile x pattern trace under every strategy.

    python scripts/collect-dataset.py --config configs/campaign.yaml [--plan] [--only RUN_ID...]

For each strategy it starts a real scheduler (``--strategy``) and the heterogeneous workers of the
config (pool sizes, simulated slowdown), all writing to PostgreSQL. It then replays each
(profile, pattern) trace, generated once with a fixed seed under ``workloads/campaign/``. Every
replayed task id carries the run's suffix, and every finished run is recorded in
``benchmark_runs`` (scenario ``campaign:<name>``). That record is how ``ml/build_dataset.py`` tags
rows with their ``run_id``, and how a second invocation skips runs already done (resumable).

``--plan`` prints the plan (deterministic for a given config) and exits.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
import random
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[1]
DSN = os.environ.get("PREDISCHED_DSN",
                     "host=localhost port=5432 dbname=predisched user=postgres password=predisched")
STRATEGY_CODES = {"round_robin": "rr", "random": "rnd", "least_loaded": "ll",
                  "resource_aware": "ra", "predictive": "pred"}


@dataclasses.dataclass(frozen=True)
class Run:
    run_id: str
    profile: str
    pattern: str
    strategy: str
    seed: int
    rate: float         # base arrivals per second of the trace
    trace: str          # repo-relative
    task_prefix: str    # every task id of the run starts with this ...
    id_suffix: str      # ... and ends with this


def load_config(path: str | Path) -> dict:
    with open(REPO / path if not Path(path).is_absolute() else path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def plan(config: dict) -> list[Run]:
    """Every run, grouped by strategy (one cluster start each); same order for the same config."""
    name = config["campaign"]
    pairs = [(p, q) for p in config["profiles"] for q in config["patterns"]]
    runs = []
    for strategy in config["strategies"]:
        for index, (profile, pattern) in enumerate(pairs):
            seed = int(config["seed"]) + index
            rate = random.Random(seed).choice(config["rates"])
            code = STRATEGY_CODES.get(strategy, strategy)
            runs.append(Run(
                run_id=f"{name}-{profile}-{pattern}-{strategy}",
                profile=profile, pattern=pattern, strategy=strategy, seed=seed, rate=rate,
                trace=f"{config['traceDir']}/{profile}-{pattern}-{seed}.jsonl",
                task_prefix=f"{profile}-{seed}-",
                id_suffix=f"~{name}-{code}"))
    return runs


class Campaign:
    def __init__(self, config: dict, java: str):
        self.config = config
        self.java = java
        self.logs = REPO / "logs" / "campaign"
        self.logs.mkdir(parents=True, exist_ok=True)
        self.nodes: list[subprocess.Popen] = []
        self.mock: subprocess.Popen | None = None

    # --- processes -------------------------------------------------------------------------
    def jar(self, module: str) -> str:
        return str(REPO / f"predisched-{module}" / "target" / f"predisched-{module}.jar")

    def client(self, *args: str, timeout: float = 600) -> subprocess.CompletedProcess:
        return subprocess.run([self.java, "-jar", self.jar("client"), "--config",
                               self.config["nodeConfig"], *args], cwd=REPO, capture_output=True,
                              text=True, timeout=timeout)

    def start_mock_http(self) -> None:
        port = self.config["mockHttp"]["port"]
        if port_open(port):
            return
        self.mock = subprocess.Popen([sys.executable, "scripts/mock-http.py", "--port", str(port)],
                                     cwd=REPO, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        wait_for(lambda: port_open(port), 15, "mock HTTP server")

    def start_cluster(self, strategy: str) -> None:
        node_config = self.config["nodeConfig"]
        sched_port = self.config["scheduler"]["port"]
        if port_open(sched_port):
            raise SystemExit(f"port {sched_port} is busy: stop the running scheduler first")
        log = open(self.logs / f"scheduler-{strategy}.log", "w", encoding="utf-8")
        self.nodes.append(subprocess.Popen(
            [self.java, "-jar", self.jar("scheduler"), "--config", node_config,
             "--port", str(sched_port), "--strategy", strategy],
            cwd=REPO, stdout=log, stderr=subprocess.STDOUT))
        wait_for(lambda: port_open(sched_port), 60, "scheduler")
        for worker in self.config["workers"]:
            ready = self.logs / f"{worker['id']}.ready"
            ready.unlink(missing_ok=True)
            log = open(self.logs / f"{worker['id']}-{strategy}.log", "w", encoding="utf-8")
            self.nodes.append(subprocess.Popen(
                [self.java, "-jar", self.jar("worker"), "--config", node_config,
                 "--id", worker["id"], "--port", str(worker["port"]),
                 "--pool-size", str(worker["poolSize"]),
                 "--slowdown", str(worker.get("slowdown", 1.0)),
                 "--ready-file", str(ready)],
                cwd=REPO, stdout=log, stderr=subprocess.STDOUT))
            wait_for(ready.exists, 60, worker["id"])
        time.sleep(3)  # first heartbeats, so every worker is dispatchable

    def stop_cluster(self) -> None:
        for proc in reversed(self.nodes):
            proc.terminate()
        for proc in self.nodes:
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()
        self.nodes = []
        time.sleep(1)

    def close(self) -> None:
        self.stop_cluster()
        if self.mock:
            self.mock.terminate()

    # --- runs ------------------------------------------------------------------------------
    def ensure_trace(self, run: Run) -> None:
        path = REPO / run.trace
        if path.exists():
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        result = self.client(
            "workload", "generate", "--profile", run.profile, "--pattern", run.pattern,
            "--tasks", str(self.config["tasksPerRun"]), "--seed", str(run.seed),
            "--rate", str(run.rate), "--profiles", self.config["profilesFile"],
            "--http-base-url", f"http://localhost:{self.config['mockHttp']['port']}",
            "--output", run.trace, timeout=120)
        if result.returncode != 0 or not path.exists():
            raise SystemExit(f"trace generation failed for {run.run_id}:\n{result.stdout}"
                             f"{result.stderr}")

    def replay(self, run: Run) -> dict:
        out = self.logs / f"{run.run_id}.csv"
        started = time.time()
        result = self.client(
            "workload", "replay", run.trace, "--speed", str(self.config["speed"]),
            "--output", str(out), "--id-suffix", run.id_suffix,
            "--timeout-ms", str(self.config["runTimeoutMs"]),
            timeout=self.config["runTimeoutMs"] / 1000 + 120)
        (self.logs / f"{run.run_id}.out").write_text(result.stdout + result.stderr,
                                                      encoding="utf-8")
        statuses: dict[str, int] = {}
        if out.exists():
            import csv
            with out.open(encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    status = row.get("status", "UNKNOWN")
                    statuses[status] = statuses.get(status, 0) + 1
        return {"trace": run.trace, "profile": run.profile, "pattern": run.pattern,
                "seed": run.seed, "rate": run.rate, "task_prefix": run.task_prefix, "id_suffix": run.id_suffix,
                "tasks": sum(statuses.values()), "statuses": statuses,
                "replay_exit": result.returncode, "wall_s": round(time.time() - started, 1),
                "workers": self.config["workers"]}


# --- database ------------------------------------------------------------------------------
def scenario(config: dict) -> str:
    return f"campaign:{config['campaign']}"


def done_runs(config: dict) -> set[str]:
    import psycopg
    with psycopg.connect(DSN) as conn:
        rows = conn.execute("SELECT run_id FROM benchmark_runs WHERE scenario = %s",
                            (scenario(config),)).fetchall()
    return {r[0] for r in rows}


def rows_for(run: Run) -> int:
    import psycopg
    with psycopg.connect(DSN) as conn:
        return conn.execute(
            "SELECT count(*) FROM execution_history WHERE task_id LIKE %s AND task_id LIKE %s",
            (run.task_prefix + "%", "%" + run.id_suffix)).fetchone()[0]


def campaign_rows(config: dict) -> int:
    import psycopg
    with psycopg.connect(DSN) as conn:
        return conn.execute("SELECT count(*) FROM execution_history WHERE task_id LIKE %s",
                            (f"%~{config['campaign']}-%",)).fetchone()[0]


def record(config: dict, run: Run, metrics: dict) -> None:
    import psycopg
    with psycopg.connect(DSN) as conn:
        conn.execute(
            "INSERT INTO benchmark_runs (run_id, strategy, scenario, metrics) VALUES"
            " (%s, %s, %s, %s) ON CONFLICT (run_id) DO UPDATE SET metrics = EXCLUDED.metrics",
            (run.run_id, run.strategy, scenario(config), json.dumps(metrics)))


# --- helpers -------------------------------------------------------------------------------
def port_open(port: int) -> bool:
    with socket.socket() as s:
        s.settimeout(0.3)
        return s.connect_ex(("127.0.0.1", port)) == 0


def wait_for(check, seconds: float, what: str) -> None:
    deadline = time.time() + seconds
    while time.time() < deadline:
        if check():
            return
        time.sleep(0.25)
    raise SystemExit(f"{what} did not come up within {seconds:.0f} s (see logs/campaign)")


def find_java() -> str:
    home = os.environ.get("JAVA_HOME")
    if home:
        for name in ("java.exe", "java"):
            candidate = Path(home) / "bin" / name
            if candidate.exists():
                return str(candidate)
    found = shutil.which("java")
    if not found:
        raise SystemExit("java not found: set JAVA_HOME or put java on PATH")
    return found


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", default="configs/campaign.yaml")
    parser.add_argument("--plan", action="store_true", help="print the plan and exit")
    parser.add_argument("--only", nargs="*", help="run only these run ids")
    args = parser.parse_args(argv)
    config = load_config(args.config)
    runs = plan(config)
    if args.only:
        runs = [r for r in runs if r.run_id in set(args.only)]
    if args.plan:
        for i, run in enumerate(runs, 1):
            print(f"{i:>3} {run.run_id}  seed={run.seed}  rate={run.rate:g}/s  {run.trace}")
        return 0

    done = done_runs(config)
    pending = [r for r in runs if r.run_id not in done]
    print(f"campaign {config['campaign']}: {len(runs)} runs planned, {len(runs) - len(pending)}"
          f" already in the database, {len(pending)} to go", flush=True)
    campaign = Campaign(config, find_java())
    try:
        campaign.start_mock_http()
        for run in runs:
            campaign.ensure_trace(run)
        position = len(runs) - len(pending)
        for strategy in config["strategies"]:
            todo = [r for r in pending if r.strategy == strategy]
            if not todo:
                continue
            campaign.start_cluster(strategy)
            try:
                for run in todo:
                    position += 1
                    metrics = campaign.replay(run)
                    time.sleep(1.5)  # the scheduler's history writer flushes every 200 ms
                    metrics["history_rows"] = rows_for(run)
                    record(config, run, metrics)
                    print(f"[{position:>2}/{len(runs)}] {run.run_id}: {metrics['tasks']} tasks"
                          f" {metrics['statuses']} -> {metrics['history_rows']} history rows"
                          f" in {metrics['wall_s']} s", flush=True)
            finally:
                campaign.stop_cluster()
    finally:
        campaign.close()
    print(f"campaign {config['campaign']} done: {campaign_rows(config)} execution rows in"
          f" execution_history", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
