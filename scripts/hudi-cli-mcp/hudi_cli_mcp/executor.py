#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Hudi CLI executor — invokes hudi-cli in script mode via subprocess."""

from __future__ import annotations

import os
import signal
import subprocess
import tempfile
import time
from dataclasses import dataclass

from hudi_cli_mcp.commands import with_spark_master
from hudi_cli_mcp.parser import ParsedOutput, parse_cli_output

# Caps on the structured output returned to the model. The CLI can emit very large
# tables (e.g. `show fsview all`, `commit showfiles` on a big table); returning all
# of it wastes context and can degrade a small model. Tunable via env vars.
DEFAULT_MAX_ROWS = int(os.environ.get("HUDI_MCP_MAX_ROWS", "200"))
DEFAULT_MAX_MESSAGES = int(os.environ.get("HUDI_MCP_MAX_MESSAGES", "100"))

# Timeout for a *write/ops* command (compaction, clustering, rollback, repair). These
# run real Spark jobs and legitimately take many minutes; the read-path default is far
# lower. Tunable via env.
DEFAULT_TIMEOUT = int(os.environ.get("HUDI_MCP_TIMEOUT", "120"))

# Timeout for executing WRITE operations (compaction/clustering runs, rollbacks,
# repairs). These launch real Spark jobs and routinely take many minutes, so they
# get a much larger budget than the read path -- a 120s kill mid-compaction would
# leave the caller unsure whether the operation partially applied.
WRITE_TIMEOUT = int(os.environ.get("HUDI_MCP_WRITE_TIMEOUT", "1800"))


@dataclass
class ExecutionResult:
    """Result of a CLI execution."""

    raw_output: str
    parsed: ParsedOutput
    return_code: int
    duration_seconds: float
    timed_out: bool = False

    def is_success(self) -> bool:
        """Whether the invocation succeeded.

        The CLI's Spring Shell script mode does not reliably propagate an
        inner-command failure to the process exit code, so a zero return code is
        necessary but not sufficient -- a captured error line (see
        :data:`hudi_cli.parser.ERROR_MARKERS`) or a timeout also means failure.
        """
        return self.return_code == 0 and not self.timed_out and not self.parsed.errors

    def to_dict(self) -> dict:
        result = self.parsed.to_dict()
        result["return_code"] = self.return_code

        # Cap rows-per-table and message/error counts so the payload stays bounded.
        truncated = False
        for table in result.get("tables", []):
            rows = table.get("rows", [])
            if len(rows) > DEFAULT_MAX_ROWS:
                table["rows"] = rows[:DEFAULT_MAX_ROWS]
                table["truncated"] = True
                truncated = True
        for key in ("messages", "errors"):
            items = result.get(key, [])
            if len(items) > DEFAULT_MAX_MESSAGES:
                result[key] = items[:DEFAULT_MAX_MESSAGES]
                truncated = True
        if truncated:
            result["notice"] = (
                f"Output truncated to {DEFAULT_MAX_ROWS} rows per table / "
                f"{DEFAULT_MAX_MESSAGES} messages. Narrow the command "
                "(e.g. --limit, a specific partition/commit) for a complete result."
            )

        result["duration_seconds"] = round(self.duration_seconds, 2)
        return result


class HudiCliExecutor:
    """Executes Hudi CLI commands via script mode.

    Each call to execute() starts a fresh JVM, runs the commands,
    and returns the parsed output.
    """

    def __init__(
        self,
        cli_bin: str | None = None,
        spark_home: str | None = None,
        cli_bundle_jar: str | None = None,
        spark_bundle_jar: str | None = None,
        timeout: int | None = None,
    ):
        self.spark_home = spark_home or os.environ.get("SPARK_HOME", "")
        self.cli_bundle_jar = cli_bundle_jar or os.environ.get("CLI_BUNDLE_JAR", "")
        self.spark_bundle_jar = spark_bundle_jar or os.environ.get(
            "SPARK_BUNDLE_JAR", ""
        )
        # No default path: a default under world-writable /tmp would let any local
        # user plant an executable the server then runs. Require it explicitly.
        self.cli_bin = cli_bin or os.environ.get("HUDI_CLI_BIN", "")
        self.default_timeout = timeout if timeout is not None else DEFAULT_TIMEOUT

        self._validate_config()

    def _validate_config(self) -> None:
        """Validate that required configuration is present."""
        missing = []
        if not self.spark_home:
            missing.append("SPARK_HOME")
        if not self.cli_bundle_jar:
            missing.append("CLI_BUNDLE_JAR")
        if not self.spark_bundle_jar:
            missing.append("SPARK_BUNDLE_JAR")
        if not self.cli_bin:
            missing.append("HUDI_CLI_BIN")
        if missing:
            raise ValueError(
                f"Missing required configuration: {', '.join(missing)}. "
                f"Set these as environment variables."
            )

        if not os.path.exists(self.cli_bin):
            raise FileNotFoundError(
                f"Hudi CLI binary not found at: {self.cli_bin}. "
                f"Check HUDI_CLI_BIN environment variable."
            )

    def execute(
        self, commands: list[str], timeout: int | None = None
    ) -> ExecutionResult:
        """Execute a list of Hudi CLI commands.

        Writes commands to a temp file, runs hudi-cli in script mode,
        captures and parses the output.
        """
        effective_timeout = timeout or self.default_timeout
        start_time = time.time()

        # Belt-and-braces against command injection: each entry must be a single
        # line. A newline here means an argument smuggled extra commands past the
        # allowlist (which validates only the first line). Refuse rather than run.
        for cmd in commands:
            if "\n" in cmd or "\r" in cmd:
                return self._error_result(
                    "Rejected: a command contained an embedded newline "
                    "(possible injection). Commands must be single-line.",
                    duration=time.time() - start_time,
                )

        cmd_file = None
        proc = None
        try:
            # Write commands to temp file
            cmd_file = tempfile.NamedTemporaryFile(
                mode="w",
                suffix=".hudi-cmd",
                delete=False,
            )
            for cmd in commands:
                cmd_file.write(with_spark_master(cmd) + "\n")
            cmd_file.close()

            # Build environment
            env = os.environ.copy()
            env["SPARK_HOME"] = self.spark_home
            env["CLI_BUNDLE_JAR"] = self.cli_bundle_jar
            env["SPARK_BUNDLE_JAR"] = self.spark_bundle_jar

            # start_new_session=True puts the wrapper shell and the JVM it spawns in
            # a new process group so a timeout can kill the *whole* tree. Without it,
            # subprocess.run's timeout kills only the wrapper shell and orphans the
            # JVM -- a "timed out" destructive op could still be completing.
            proc = subprocess.Popen(
                [self.cli_bin, "script", "--file", cmd_file.name],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env,
                start_new_session=True,
            )
            try:
                stdout, stderr = proc.communicate(timeout=effective_timeout)
            except subprocess.TimeoutExpired:
                self._kill_group(proc)
                return self._timeout_result(effective_timeout, time.time() - start_time)

            duration = time.time() - start_time
            raw_output = (stdout or "") + "\n" + (stderr or "")
            parsed = parse_cli_output(raw_output)

            return ExecutionResult(
                raw_output=raw_output,
                parsed=parsed,
                return_code=proc.returncode,
                duration_seconds=duration,
            )

        except FileNotFoundError:
            return self._error_result(
                f"Hudi CLI binary not found at: {self.cli_bin}. "
                f"Check HUDI_CLI_BIN configuration.",
                duration=time.time() - start_time,
            )

        finally:
            if cmd_file and os.path.exists(cmd_file.name):
                os.unlink(cmd_file.name)

    @staticmethod
    def _kill_group(proc: subprocess.Popen) -> None:
        """Terminate the process group (wrapper shell + JVM grandchild)."""
        try:
            pgid = os.getpgid(proc.pid)
        except ProcessLookupError:
            return
        try:
            os.killpg(pgid, signal.SIGTERM)
            proc.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(pgid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        except ProcessLookupError:
            pass

    @staticmethod
    def _error_result(message: str, duration: float) -> ExecutionResult:
        return ExecutionResult(
            raw_output=message,
            parsed=ParsedOutput(errors=[message]),
            return_code=-1,
            duration_seconds=duration,
        )

    @staticmethod
    def _lazy_error(message: str) -> ExecutionResult:
        return ExecutionResult(
            raw_output=message,
            parsed=ParsedOutput(errors=[message]),
            return_code=-1,
            duration_seconds=0.0,
        )

    @staticmethod
    def _timeout_result(effective_timeout: int, duration: float) -> ExecutionResult:
        msg = (
            f"Command timed out after {effective_timeout}s and was killed. "
            "If this was a write/compaction/clustering/rollback, it may have "
            "partially applied -- inspect the timeline (commits show, "
            "compactions show all) before retrying rather than re-running blindly. "
            "For long-running jobs, raise HUDI_MCP_TIMEOUT."
        )
        return ExecutionResult(
            raw_output=msg,
            parsed=ParsedOutput(errors=[msg]),
            return_code=-1,
            duration_seconds=duration,
            timed_out=True,
        )


class LazyHudiCliExecutor:
    """Defers building the real executor until the first command runs.

    ``HudiCliExecutor.__init__`` validates SPARK_HOME / CLI_BUNDLE_JAR /
    SPARK_BUNDLE_JAR / HUDI_CLI_BIN and raises if any is missing. Constructing it
    eagerly at import time means the whole MCP server dies on startup on a
    misconfig -- and the MCP stdio transport passes only a sanitized environment,
    so the child process often does not inherit those vars. Deferring construction
    keeps the server alive and turns a misconfig into a per-call error the client
    can read (and recover from once the environment is fixed) instead of a dead
    server. Construction is retried each call until it succeeds.
    """

    def __init__(self, **kwargs: object) -> None:
        self._kwargs = kwargs
        self._real: HudiCliExecutor | None = None

    def execute(
        self, commands: list[str], timeout: int | None = None
    ) -> ExecutionResult:
        if self._real is None:
            try:
                self._real = HudiCliExecutor(**self._kwargs)  # type: ignore[arg-type]
            except (ValueError, FileNotFoundError) as e:
                return HudiCliExecutor._lazy_error(f"Hudi CLI is not configured: {e}")
        return self._real.execute(commands, timeout=timeout)
