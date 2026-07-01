"""Hudi CLI executor — invokes hudi-cli in script mode via subprocess."""

from __future__ import annotations

import os
import subprocess
import tempfile
import time
from dataclasses import dataclass

from hudi_cli.parser import ParsedOutput, parse_cli_output


@dataclass
class ExecutionResult:
    """Result of a CLI execution."""

    raw_output: str
    parsed: ParsedOutput
    return_code: int
    duration_seconds: float

    def to_dict(self) -> dict:
        result = self.parsed.to_dict()
        result["return_code"] = self.return_code
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
        timeout: int = 120,
    ):
        self.spark_home = spark_home or os.environ.get("SPARK_HOME", "")
        self.cli_bundle_jar = cli_bundle_jar or os.environ.get("CLI_BUNDLE_JAR", "")
        self.spark_bundle_jar = spark_bundle_jar or os.environ.get(
            "SPARK_BUNDLE_JAR", ""
        )
        self.cli_bin = cli_bin or os.environ.get(
            "HUDI_CLI_BIN", "/tmp/hudi-cli-staging/hudi-cli-with-bundle.sh"
        )
        self.default_timeout = timeout

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
        cmd_file = None
        start_time = time.time()

        try:
            # Write commands to temp file
            cmd_file = tempfile.NamedTemporaryFile(
                mode="w",
                suffix=".hudi-cmd",
                delete=False,
            )
            for cmd in commands:
                cmd_file.write(cmd + "\n")
            cmd_file.close()

            # Build environment
            env = os.environ.copy()
            env["SPARK_HOME"] = self.spark_home
            env["CLI_BUNDLE_JAR"] = self.cli_bundle_jar
            env["SPARK_BUNDLE_JAR"] = self.spark_bundle_jar

            # Execute CLI in script mode
            result = subprocess.run(
                [self.cli_bin, "script", "--file", cmd_file.name],
                capture_output=True,
                text=True,
                timeout=effective_timeout,
                env=env,
            )

            duration = time.time() - start_time
            raw_output = result.stdout + "\n" + result.stderr
            parsed = parse_cli_output(raw_output)

            return ExecutionResult(
                raw_output=raw_output,
                parsed=parsed,
                return_code=result.returncode,
                duration_seconds=duration,
            )

        except subprocess.TimeoutExpired:
            duration = time.time() - start_time
            return ExecutionResult(
                raw_output=f"Command timed out after {effective_timeout}s",
                parsed=ParsedOutput(
                    messages=[
                        f"Command timed out after {effective_timeout}s. "
                        f"Try reducing --limit or simplifying the command."
                    ]
                ),
                return_code=-1,
                duration_seconds=duration,
            )

        except FileNotFoundError:
            duration = time.time() - start_time
            return ExecutionResult(
                raw_output=f"CLI binary not found: {self.cli_bin}",
                parsed=ParsedOutput(
                    messages=[
                        f"Hudi CLI binary not found at: {self.cli_bin}. "
                        f"Check HUDI_CLI_BIN configuration."
                    ]
                ),
                return_code=-1,
                duration_seconds=duration,
            )

        finally:
            if cmd_file and os.path.exists(cmd_file.name):
                os.unlink(cmd_file.name)
