#!/usr/bin/env python3
"""
Fuzz test runner for lz4-java

Note: 100% AI generated, don't try to decipher it ^^

Features:
- Enumerates all fuzz surefire executions from pom.xml (profile: fuzz)
- Runs each fuzz test as an individual Maven command
- Parallel execution with configurable concurrency
- Configurable Jazzer duration per test
- Isolates each run's Maven target directory via -Dproject.build.directory to avoid interference
- In-progress status updates (live summary)
- Final JSON and HTML reports with per-test logs and findings

Usage:
  python3 scripts/fuzz_runner.py [options]

Examples:
  # Dry list of detected fuzz tests
  python3 scripts/fuzz_runner.py --list

  # Run all with 5s per test, 4-way parallel
  python3 scripts/fuzz_runner.py -j 4 -d 5s

  # Filter tests by substring (on execution id or <test> value)
  python3 scripts/fuzz_runner.py -f LZ4DecompressorTest#native_fast_array

Notes:
- Uses ./mvnw by default (recommended per project guide).
- Writes per-test outputs into <out_dir>/<execId-sanitized>.
- Also sets JAZZER_FINDINGS_DIR per-run to keep findings isolated.

"""

import argparse
import asyncio
import dataclasses
import datetime as dt
import html
import json
import os
import re
import shutil
import signal
import sys
import time
import traceback
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Optional, Tuple

POM_NS = {"m": "http://maven.apache.org/POM/4.0.0"}


@dataclasses.dataclass
class FuzzExecution:
    execution_id: str  # surefire execution id (unique)
    test_spec: str     # value in <test> (e.g., net.jpountz.fuzz.LZ4DecompressorTest#safe_fast_array)
    surefire_version: str
    profile_id: str = "fuzz"

    @property
    def test_class(self) -> Optional[str]:
        parts = self.test_spec.split("#", 1)
        return parts[0] if parts else None

    @property
    def test_method(self) -> Optional[str]:
        parts = self.test_spec.split("#", 1)
        return parts[1] if len(parts) == 2 else None


@dataclasses.dataclass
class JobResult:
    status: str  # queued, running, passed, failed, cancelled
    execution: FuzzExecution
    build_dir: Path
    findings_dir: Path
    log_path: Path
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    exit_code: Optional[int] = None
    command: Optional[List[str]] = None
    last_lines: List[str] = dataclasses.field(default_factory=list)
    surefire_report_dir: Optional[Path] = None


def sanitize_for_path(s: str) -> str:
    # Avoid overly long paths but keep uniqueness by hashing tail
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", s)
    if len(safe) > 150:
        import hashlib
        h = hashlib.sha1(safe.encode()).hexdigest()[:8]
        safe = safe[:120] + "_" + h
    return safe


def read_pom_executions(pom_path: Path, profile_id: str = "fuzz") -> List[FuzzExecution]:
    tree = ET.parse(str(pom_path))
    root = tree.getroot()

    # Find the target profile
    profile_node = None
    for p in root.findall("m:profiles/m:profile", POM_NS):
        pid = p.findtext("m:id", default="", namespaces=POM_NS)
        if pid == profile_id:
            profile_node = p
            break
    if profile_node is None:
        raise RuntimeError(f"Profile '{profile_id}' not found in {pom_path}")

    surefire_nodes = []
    surefire_version = None
    for plugin in profile_node.findall("m:build/m:plugins/m:plugin", POM_NS):
        artifact_id = plugin.findtext("m:artifactId", default="", namespaces=POM_NS)
        group_id = plugin.findtext("m:groupId", default="", namespaces=POM_NS)
        if artifact_id == "maven-surefire-plugin":
            surefire_nodes.append(plugin)
            v = plugin.findtext("m:version", default="", namespaces=POM_NS)
            m = re.fullmatch(r"\$\{(.+)\}", v)
            if m:
                v = root.findtext(f"m:properties/m:{m.group(1)}", default="", namespaces=POM_NS)
            if v:
                surefire_version = v

    if not surefire_nodes:
        raise RuntimeError("No maven-surefire-plugin found in fuzz profile")

    if not surefire_version:
        # Fallback to commonly used version in this repo
        surefire_version = "3.2.5"

    executions: List[FuzzExecution] = []
    for sn in surefire_nodes:
        for exec_node in sn.findall("m:executions/m:execution", POM_NS):
            exec_id = exec_node.findtext("m:id", default="", namespaces=POM_NS)
            # Only include executions with explicit <test> selection
            test_spec = exec_node.findtext("m:configuration/m:test", default="", namespaces=POM_NS)
            if exec_id and test_spec:
                executions.append(FuzzExecution(exec_id, test_spec, surefire_version, profile_id))
    if not executions:
        raise RuntimeError("No fuzz executions with <test> found in fuzz profile")
    return executions


def filter_executions(execs: List[FuzzExecution], pattern: Optional[str]) -> List[FuzzExecution]:
    if not pattern:
        return execs
    pat = pattern
    out = []
    for e in execs:
        if pat in e.execution_id or pat in e.test_spec:
            out.append(e)
    return out


def detect_mvnw(base_dir: Path) -> Path:
    mvnw = base_dir / "mvnw"
    if mvnw.exists():
        return mvnw
    mvn = shutil.which("mvn")
    if mvn:
        return Path(mvn)
    raise RuntimeError("Neither ./mvnw nor mvn found in PATH")



async def stream_process(cmd: List[str], cwd: Path, env: Dict[str, str], log_file: Path, result: JobResult):
    # Capture combined stdout/err
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        cwd=str(cwd),
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    result.start_time = time.time()
    result.status = "running"

    # Simple tail of last N lines for status
    tail_max = 8
    last_lines: List[str] = []

    # Ensure log dir
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("wb") as f:
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            f.write(line)
            # update tail
            sline = line.decode(errors="replace").rstrip()
            last_lines.append(sline)
            if len(last_lines) > tail_max:
                last_lines.pop(0)
            result.last_lines = last_lines[:]

    exit_code = await proc.wait()
    result.exit_code = exit_code
    result.end_time = time.time()
    result.status = "passed" if exit_code == 0 else "failed"


def build_maven_command(mvnw: Path, execution: FuzzExecution, build_dir: Path, duration: str, batch: bool, rss_limit_mb: int, xmx_mb: int) -> List[str]:
    cmd: List[str] = [str(mvnw), "-P", execution.profile_id]
    if batch:
        cmd += ["-B"]
    # Enforce per-test memory limits:
    # - libFuzzer RSS limit via JAZZER_FLAGS
    # - ASan hard RSS limit so native allocations are capped too
    jazzer_flags = f"-rss_limit_mb={rss_limit_mb}"
    asan_opts = f"detect_leaks=1,abort_on_error=1,fast_unwind_on_malloc=0,hard_rss_limit_mb={rss_limit_mb}"
    cmd += [
        f"-Djazzer.max_duration={duration}",
        f"-Denv.JAZZER_FLAGS={jazzer_flags}",
        f"-Denv.ASAN_OPTIONS={asan_opts}",
        # Constrain heap of the forked Surefire JVM to avoid hitting RSS limits due to Java heap growth
        f"-Denv.JAVA_TOOL_OPTIONS=-Xmx{xmx_mb}m",
        f"-DargLine=-Xmx{xmx_mb}m",
        # Run the single surefire execution by id
        f"org.apache.maven.plugins:maven-surefire-plugin:{execution.surefire_version}:test@{execution.execution_id}",
    ]
    return cmd


async def run_job(
    base_dir: Path,
    mvnw: Path,
    execution: FuzzExecution,
    duration: str,
    env_base: Dict[str, str],
    batch: bool,
    result: JobResult,
    rss_limit_mb: int,
    xmx_mb: int,
) -> JobResult:
    # result is pre-populated with paths for this execution
    result.status = "queued"

    # Prepare environment
    env = dict(env_base)
    # Ensure findings go per-job
    env["JAZZER_FINDINGS_DIR"] = str(result.findings_dir)
    # Reinforce JAZZER_FUZZ=1 (pom also sets it, but merge-in is fine)
    env.setdefault("JAZZER_FUZZ", "1")
    # Enforce libFuzzer per-test RSS limit (1 GiB default, configurable)
    env["JAZZER_FLAGS"] = f"-rss_limit_mb={rss_limit_mb}"

    # Prepare isolated working copy per job to eliminate cross-run interference
    job_root = Path(str(result.log_path)).parent  # job_dir
    work_dir = job_root / "work"

    # Always refresh from the current repository state so changes are visible in isolated runs
    def _ignore(src_dir, names):
        ignored = {n for n in names if n in (".git", ".idea", "target", "fuzz-out")}
        ignored |= {n for n in names if n.startswith("hs_err_pid") or n.startswith("crash-")}
        return ignored

    if work_dir.exists():
        shutil.rmtree(work_dir)
    shutil.copytree(str(base_dir), str(work_dir), ignore=_ignore)

    # Reset result directories to the isolated work dir target
    result.build_dir = work_dir / "target"
    result.surefire_report_dir = result.build_dir / "surefire-reports"

    # 1) compile step without fuzz env or memory limits
    env_compile = dict(env_base)
    for k in ["JAZZER_FUZZ", "JAZZER_FLAGS", "ASAN_OPTIONS", "JAVA_TOOL_OPTIONS"]:
        env_compile.pop(k, None)
    cmd_compile = [str(mvnw), "-P", execution.profile_id]
    if batch:
        cmd_compile += ["-B"]
    cmd_compile += ["test-compile"]
    result.command = cmd_compile
    await stream_process(cmd_compile, work_dir, env_compile, result.log_path, result)
    if result.exit_code != 0:
        return result

    # 2) surefire execution with fuzz env and limits
    cmd = build_maven_command(mvnw, execution, result.build_dir, duration, batch, rss_limit_mb, xmx_mb)
    result.command = cmd

    try:
        await stream_process(cmd, work_dir, env, result.log_path, result)
    except asyncio.CancelledError:
        result.status = "cancelled"
        result.end_time = time.time()
        # Try to annotate log
        with result.log_path.open("a", encoding="utf-8", errors="replace") as f:
            f.write("\n\n[Cancelled]\n")
        raise
    except Exception:
        result.status = "failed"
        result.end_time = time.time()
        with result.log_path.open("a", encoding="utf-8", errors="replace") as f:
            f.write("\n\n[Runner Exception]\n")
            f.write(traceback.format_exc())
    return result


def human_duration(seconds: float) -> str:
    if seconds < 1:
        return f"{int(seconds * 1000)}ms"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h{m}m{s}s"
    if m:
        return f"{m}m{s}s"
    return f"{s}s"


def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).astimezone().isoformat()


async def status_printer(results: Dict[str, JobResult], start_time: float, refresh: float = 1.0):
    spinner = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
    i = 0
    last_lines_to_show = 1

    def summarize() -> Tuple[int, int, int, int]:
        total = len(results)
        passed = sum(1 for r in results.values() if r.status == "passed")
        failed = sum(1 for r in results.values() if r.status == "failed")
        running = sum(1 for r in results.values() if r.status == "running")
        return total, passed, failed, running

    while True:
        await asyncio.sleep(refresh)
        total, passed, failed, running = summarize()
        elapsed = human_duration(time.time() - start_time)
        # Print summary and a few running tests with last line
        lines = []
        lines.append(f"[{spinner[i % len(spinner)]}] total={total} running={running} passed={passed} failed={failed} elapsed={elapsed}")
        i += 1

        running_rows = [(k, v) for k, v in results.items() if v.status == "running"]
        running_rows = sorted(running_rows, key=lambda kv: kv[1].start_time or 0.0)
        for key, r in running_rows[:5]:  # show up to 5
            elapsed_r = human_duration((time.time() - (r.start_time or time.time())))
            tail = (" | ".join(r.last_lines[-last_lines_to_show:])) if r.last_lines else ""
            lines.append(f"  - {r.execution.execution_id} [{elapsed_r}] {tail}")

        # Use stderr to avoid mixing with per-process stdout capture and keep simple
        sys.stderr.write("\n".join(lines) + "\n")
        sys.stderr.flush()

        # Exit condition handled by caller; status_printer will be cancelled


def make_report_json(
    results: Dict[str, JobResult],
    project: str,
    profile: str,
    duration: str,
    concurrency: int,
    mvn_cmd: str,
    out_dir: Path,
    base_dir: Path,
) -> Dict:
    total = len(results)
    passed = sum(1 for r in results.values() if r.status == "passed")
    failed = sum(1 for r in results.values() if r.status == "failed")
    cancelled = sum(1 for r in results.values() if r.status == "cancelled")
    start_ts = min((r.start_time or time.time()) for r in results.values()) if results else time.time()
    end_ts = max((r.end_time or time.time()) for r in results.values()) if results else time.time()

    # helper to relativize paths
    def rel(p: Optional[Path]) -> Optional[str]:
        if not p:
            return None
        try:
            return os.path.relpath(str(p), start=str(base_dir))
        except Exception:
            return str(p)

    tests = []
    for k, r in sorted(results.items()):
        tests.append({
            "id": r.execution.execution_id,
            "test": r.execution.test_spec,
            "class": r.execution.test_class,
            "method": r.execution.test_method,
            "status": r.status,
            "exit_code": r.exit_code,
            "started_at": dt.datetime.fromtimestamp(r.start_time or 0, tz=dt.timezone.utc).astimezone().isoformat() if r.start_time else None,
            "ended_at": dt.datetime.fromtimestamp(r.end_time or 0, tz=dt.timezone.utc).astimezone().isoformat() if r.end_time else None,
            "duration_seconds": (r.end_time - r.start_time) if (r.end_time and r.start_time) else None,
            "build_dir": rel(r.build_dir),
            "surefire_report_dir": rel(r.surefire_report_dir) if r.surefire_report_dir else None,
            "findings_dir": rel(r.findings_dir),
            "log_path": rel(r.log_path),
            "command": r.command,
        })

    return {
        "generated_at": now_iso(),
        "project": project,
        "profile": profile,
        "jazzer_duration": duration,
        "concurrency": concurrency,
        "maven_command": mvn_cmd,
        "out_dir": os.path.relpath(str(out_dir), start=str(base_dir)),
        "summary": {
            "total": total,
            "passed": passed,
            "failed": failed,
            "cancelled": cancelled,
            "duration_seconds": end_ts - start_ts if total else 0,
        },
        "tests": tests,
    }


def make_report_html(report_json: Dict) -> str:
    # Embed JSON and a tiny viewer
    data_js = "window.REPORT = " + json.dumps(report_json, indent=2) + ";"
    html_doc = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Fuzz Report</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 20px; }}
    .summary {{ margin-bottom: 16px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ padding: 6px 8px; border-bottom: 1px solid #ddd; text-align: left; font-size: 13px; }}
    tr:hover {{ background: #fafafa; }}
    .status-passed {{ color: #0a0; font-weight: 600; }}
    .status-failed {{ color: #a00; font-weight: 600; }}
    .status-cancelled {{ color: #a60; font-weight: 600; }}
    code {{ background: #f5f5f5; padding: 2px 4px; border-radius: 3px; }}
    .small {{ color: #666; font-size: 12px; }}
    .filter {{ margin-bottom: 10px; }}
    .nowrap {{ white-space: nowrap; }}
  </style>
</head>
<body>
  <h2>Fuzz Report</h2>
  <div class="summary" id="summary"></div>

  <div class="filter">
    Filter: <input type="text" id="filter" placeholder="Substring in id/test/class/method/status">
  </div>

  <table id="tbl">
    <thead>
      <tr>
        <th>Status</th>
        <th>Execution ID</th>
        <th>Test</th>
        <th>Duration</th>
        <th>Build Dir</th>
        <th>Logs</th>
        <th>Findings</th>
        <th>Surefire Reports</th>
      </tr>
    </thead>
    <tbody></tbody>
  </table>

  <script>
  {data_js}
  function esc(s) {{
    return (s||"").toString().replace(/[&<>"]/g, c => ({{"&":"&amp;","<":"&lt;",">":"&gt;","\\"":"&quot;"}})[c]);
  }}
  function humanDuration(sec) {{
    if (!sec && sec !== 0) return "";
    if (sec < 1) return Math.round(sec*1000) + "ms";
    var m = Math.floor(sec/60), s = Math.floor(sec % 60), h = Math.floor(m/60); m = m % 60;
    if (h) return h + "h" + m + "m" + s + "s";
    if (m) return m + "m" + s + "s";
    return s + "s";
  }}
  function render() {{
    const rep = window.REPORT;
    // Compute a prefix that navigates from the report file location (rep.out_dir/...) back to project root
    // so that project-root-relative paths (like t.log_path) resolve correctly in the browser.
    const prefix = (function () {{
      const out = (rep.out_dir || "").split("/").filter(Boolean);
      if (out.length === 0) return "";
      let p = "";
      for (let i = 0; i < out.length; i++) p += "../";
      return p;
    }})();
    const sum = rep.summary;
    const summaryEl = document.getElementById("summary");
    summaryEl.innerHTML =
      "<div><strong>Project:</strong> " + esc(rep.project) + " " +
      "<span class='small'>(" + esc(rep.profile) + ", duration=" + esc(rep.jazzer_duration) + ", concurrency=" + rep.concurrency + ")</span></div>" +
      "<div><strong>Total:</strong> " + sum.total +
      " &nbsp; <span class='status-passed'>Passed:</span> " + sum.passed +
      " &nbsp; <span class='status-failed'>Failed:</span> " + sum.failed +
      " &nbsp; <span class='status-cancelled'>Cancelled:</span> " + sum.cancelled +
      " &nbsp; <span class='small'>Runtime: " + humanDuration(sum.duration_seconds) + "</span></div>" +
      "<div class='small'>Output dir: <code>" + esc(rep.out_dir) + "</code></div>";

    const filter = document.getElementById("filter").value.toLowerCase();
    const tbody = document.querySelector("#tbl tbody");
    tbody.innerHTML = "";
    rep.tests.forEach(t => {{
      const hay = (t.id + " " + t.test + " " + (t.class||"") + " " + (t.method||"") + " " + t.status).toLowerCase();
      if (filter && hay.indexOf(filter) === -1) return;
      const tr = document.createElement("tr");
      const stClass = "status-" + (t.status||"unknown");
      // Build href relative to the report directory: if log_path starts with out_dir/, strip it; else prefix up to root.
      const href = (function () {{
        const p = t.log_path || "";
        const r = rep.out_dir || "";
        if (r && p.startsWith(r + "/")) return p.slice(r.length + 1);
        return prefix + p;
      }})();
      tr.innerHTML =
        "<td class='" + stClass + " nowrap'>" + esc(t.status) + "</td>" +
        "<td class='nowrap'>" + esc(t.id) + "</td>" +
        "<td><code>" + esc(t.test) + "</code></td>" +
        "<td class='nowrap'>" + humanDuration(t.duration_seconds) + "</td>" +
        "<td><code>" + esc(t.build_dir || "") + "</code></td>" +
        "<td>" + (t.log_path ? ("<a href='" + esc(href) + "' target='_blank'>build.log</a>") : "") + "</td>" +
        "<td><code>" + esc(t.findings_dir || "") + "</code></td>" +
        "<td><code>" + esc(t.surefire_report_dir || "") + "</code></td>";
      tbody.appendChild(tr);
    }});
  }}
  document.getElementById("filter").addEventListener("input", render);
  render();
  </script>
</body>
</html>
"""
    return html_doc


async def main_async(args):
    base_dir = Path(args.base_dir).resolve()
    pom_path = base_dir / "pom.xml"
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    mvnw = Path(args.mvn) if args.mvn else detect_mvnw(base_dir)
    mvn_cmd_str = str(mvnw)

    execs = read_pom_executions(pom_path, profile_id=args.profile)
    execs = filter_executions(execs, args.filter)

    if args.list:
        print(f"Detected {len(execs)} fuzz executions in profile '{args.profile}':")
        for e in execs:
            print(f"- {e.execution_id}  [{e.surefire_version}]  test={e.test_spec}")
        return 0

    if not execs:
        print("No fuzz executions after filtering.", file=sys.stderr)
        return 1

    # Shared environment
    env_base = dict(os.environ)

    # Prepare jobs map
    results: Dict[str, JobResult] = {
        e.execution_id: JobResult(
            status="queued",
            execution=e,
            build_dir=out_dir / sanitize_for_path(e.execution_id) / "target",
            findings_dir=out_dir / sanitize_for_path(e.execution_id) / "findings",
            log_path=out_dir / sanitize_for_path(e.execution_id) / "build.log",
            surefire_report_dir=out_dir / sanitize_for_path(e.execution_id) / "target" / "surefire-reports",
        )
        for e in execs
    }

    # Concurrency control
    sem = asyncio.Semaphore(args.jobs)

    async def run_one(e: FuzzExecution):
        async with sem:
            return await run_job(
                base_dir=base_dir,
                mvnw=mvnw,
                execution=e,
                duration=args.duration,
                env_base=env_base,
                batch=not args.no_batch,
                result=results[e.execution_id],
                rss_limit_mb=args.rss_limit_mb,
                xmx_mb=args.xmx_mb,
            )

    # Kick off tasks
    start_time = time.time()
    loop = asyncio.get_running_loop()
    tasks = {}
    for e in execs:
        task = asyncio.create_task(run_one(e))
        tasks[e.execution_id] = task

    # Status printer
    printer_task = asyncio.create_task(status_printer(results, start_time, refresh=args.refresh))

    # As tasks finish, update results dict entries (the JobResult returned carries all details)
    done, pending = await asyncio.wait(tasks.values(), return_when=asyncio.FIRST_COMPLETED)
    # Replace placeholder results with real ones as they complete
    # We will do it in a loop until all tasks done
    while tasks:
        done, _pending = await asyncio.wait(tasks.values(), return_when=asyncio.FIRST_COMPLETED)
        for t in done:
            # Find which exec this belongs to
            exec_id = None
            for k, v in list(tasks.items()):
                if v is t:
                    exec_id = k
                    break
            try:
                res = t.result()
            except Exception as ex:
                # Create a failed result entry
                e = next((ev for ev in execs if ev.execution_id == exec_id), None)
                if e:
                    res = JobResult(
                        status="failed",
                        execution=e,
                        build_dir=out_dir / sanitize_for_path(e.execution_id) / "target",
                        findings_dir=out_dir / sanitize_for_path(e.execution_id) / "findings",
                        log_path=out_dir / sanitize_for_path(e.execution_id) / "build.log",
                        surefire_report_dir=out_dir / sanitize_for_path(e.execution_id) / "target" / "surefire-reports",
                        start_time=start_time,
                        end_time=time.time(),
                        exit_code=-1,
                    )
                    with res.log_path.open("a", encoding="utf-8", errors="replace") as f:
                        f.write("\n[Runner captured exception]\n")
                        f.write("".join(traceback.format_exception(ex)))
                else:
                    continue
            if exec_id:
                results[exec_id] = res
                del tasks[exec_id]

    # Stop status printer
    if not printer_task.done():
        printer_task.cancel()
        with contextlib_suppress(asyncio.CancelledError):
            await printer_task

    # Write reports
    project_name = "lz4-java"
    report_json = make_report_json(
        results=results,
        project=project_name,
        profile=args.profile,
        duration=args.duration,
        concurrency=args.jobs,
        mvn_cmd=mvn_cmd_str,
        out_dir=out_dir,
        base_dir=base_dir,
    )
    json_path = Path(args.json_report).resolve()
    html_path = Path(args.html_report).resolve()
    json_path.parent.mkdir(parents=True, exist_ok=True)
    html_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(report_json, indent=2), encoding="utf-8")
    html_path.write_text(make_report_html(report_json), encoding="utf-8")

    # Final console summary
    total = len(results)
    passed = sum(1 for r in results.values() if r.status == "passed")
    failed = sum(1 for r in results.values() if r.status == "failed")
    cancelled = sum(1 for r in results.values() if r.status == "cancelled")
    print(f"\n=== Fuzz summary: total={total} passed={passed} failed={failed} cancelled={cancelled}")
    print(f"JSON report: {json_path}")
    print(f"HTML report: {html_path}")

    return 0 if failed == 0 else 2


class contextlib_suppress:
    # Minimal stand-in for contextlib.suppress to avoid import if Python < 3.11 environment quirks
    def __init__(self, *exceptions):
        self.exceptions = exceptions or (Exception,)
    def __enter__(self): return self
    def __exit__(self, exctype, exc, tb):
        return exctype is not None and issubclass(exctype, self.exceptions)


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run lz4-java fuzz tests in parallel with isolated Maven executions.")
    parser.add_argument("--base-dir", default=".", help="Project base directory that contains pom.xml (default: .)")
    parser.add_argument("--mvn", default=None, help="Path to mvn/mvnw (default: auto-detect ./mvnw then mvn in PATH)")
    parser.add_argument("--profile", default="fuzz", help="Maven profile to use (default: fuzz)")
    parser.add_argument("-j", "--jobs", type=int, default=os.cpu_count() or 4, help="Max parallel jobs (default: CPU count)")
    parser.add_argument("-d", "--duration", default="5s", help="Jazzer duration per fuzz test, e.g. 5s, 30s, 2m (default: 5s)")
    parser.add_argument("-o", "--out-dir", default="fuzz-out", help="Output directory for per-test results (default: fuzz-out)")
    parser.add_argument("--json-report", default="fuzz-out/fuzz-report.json", help="Path to write final JSON report")
    parser.add_argument("--html-report", default="fuzz-out/fuzz-report.html", help="Path to write final HTML report")
    parser.add_argument("-f", "--filter", default=None, help="Substring filter applied to execution id or test spec")
    parser.add_argument("--list", action="store_true", help="List detected fuzz tests and exit")
    parser.add_argument("--no-batch", action="store_true", help="Do not pass -B to Maven (interactive/verbose)")
    parser.add_argument("--refresh", type=float, default=1.0, help="Status refresh interval seconds (default: 1.0)")
    parser.add_argument("--rss-limit-mb", type=int, default=1024, help="Per-test memory cap in MB enforced via libFuzzer (-rss_limit_mb) and ASAN (hard_rss_limit_mb) (default: 1024)")
    parser.add_argument("--xmx-mb", type=int, default=700, help="Max Java heap (-Xmx) for the forked test JVM in MB (default: 700)")
    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    # Quick validation of duration format (very lenient)
    if not re.match(r"^[0-9]+(ms|s|m|h)?$", args.duration):
        print(f"Warning: duration '{args.duration}' may not be valid for jazzer.max_duration", file=sys.stderr)

    # Trap SIGINT to attempt graceful cancel
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    stop = asyncio.Event()

    def handle_sigint():
        try:
            loop.remove_signal_handler(signal.SIGINT)
        except Exception:
            pass
        stop.set()

    try:
        loop.add_signal_handler(signal.SIGINT, handle_sigint)
    except NotImplementedError:
        # Windows or limited environment
        pass

    try:
        rc = loop.run_until_complete(main_async(args))
    finally:
        loop.close()
    return rc


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
