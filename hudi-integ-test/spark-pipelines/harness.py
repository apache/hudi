# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Owned, foreground Spark Connect server and bounded SDP client processes (POSIX)."""

import argparse
from collections import deque
import contextlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time


SPARK_VERSION = "4.1.1"
HERE = Path(__file__).resolve().parent


@contextlib.contextmanager
def owned_process(command, log, cwd, env):
    """Kill only the new process group, including children left by an exited launcher."""
    with log.open("w") as output:
        output.write("Command: " + repr([str(arg) for arg in command]) + "\n")
        output.flush()
        process = subprocess.Popen(command, cwd=cwd, env=env, stdout=output,
                                   stderr=subprocess.STDOUT, start_new_session=True)
        try:
            yield process
        finally:
            stop_process_group(process)


def stop_process_group(process, grace=5):
    # Retain the leader until its group has been signalled; never use pkill or Spark's global stop script.
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        process.wait()
        return
    deadline = time.monotonic() + grace
    while time.monotonic() < deadline:
        process.poll()
        try:
            os.killpg(process.pid, 0)
        except ProcessLookupError:
            break
        time.sleep(0.1)
    else:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
    process.wait(timeout=grace)


def wait_for_process(process, timeout, log, server=None):
    deadline = time.monotonic() + timeout
    while process.poll() is None:
        if server is not None and server.poll() is not None:
            raise RuntimeError(f"Connect server exited with {server.returncode}; see server.log and {log}")
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Client exceeded {timeout}s; see {log}")
        time.sleep(0.1)
    if process.returncode:
        raise RuntimeError(f"Client exited with {process.returncode}; see {log}")


class ConnectHarness:
    def __init__(self, bundle, logs, startup_timeout=90, client_timeout=180):
        import pyspark

        if pyspark.__version__ != SPARK_VERSION:
            raise ValueError(f"Install requirements.txt: expected PySpark {SPARK_VERSION}, got {pyspark.__version__}")
        self.spark_home = Path(pyspark.__file__).resolve().parent
        self.bundle = Path(bundle).resolve(strict=True)
        if not self.bundle.is_file() or self.bundle.suffix != ".jar":
            raise ValueError("--bundle must name the built Hudi Spark 4.1 bundle jar")
        self.logs = Path(logs).resolve()
        self.logs.mkdir(parents=True, exist_ok=True)
        self.startup_timeout = startup_timeout
        self.client_timeout = client_timeout
        self.server = None
        self.stack = contextlib.ExitStack()

    def __enter__(self):
        try:
            self.root = Path(self.stack.enter_context(tempfile.TemporaryDirectory(prefix="hudi-sdp-")))
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                self.port = sock.getsockname()[1]
            self.remote = f"sc://127.0.0.1:{self.port}"
            self.env = self.process_environment()
            conf = self.server_configuration()
            (self.logs / "server-conf.json").write_text(json.dumps(conf, indent=2) + "\n")
            command = [str(self.spark_home / "bin/spark-submit"), "--master", "local[2]",
                       "--driver-memory", "1g", "--jars", str(self.bundle)]
            for key, value in conf.items():
                command.extend(["--conf", f"{key}={value}"])
            command.extend(["--class", "org.apache.spark.sql.connect.service.SparkConnectServer"])
            self.server = self.stack.enter_context(owned_process(
                command, self.logs / "server.log", self.root, self.env))
            # A TCP check alone can accept an unrelated listener. The probe below also verifies our run ID.
            self.wait_for_port()
            self.client(HERE / "client.py", "probe", timeout=self.startup_timeout)
            return self
        except BaseException:
            self.stack.close()
            raise

    def __exit__(self, *exc):
        self.stack.close()

    def process_environment(self):
        env = os.environ.copy()
        for key in ("SPARK_REMOTE", "SPARK_CONNECT_MODE_ENABLED", "SPARK_LOCAL_REMOTE", "SPARK_CONNECT_MODE",
                    "PYSPARK_SUBMIT_ARGS", "PYSPARK_GATEWAY_PORT", "PYSPARK_GATEWAY_SECRET",
                    "SPARK_SUBMIT_OPTS", "SPARK_JAVA_OPTS", "PYTHONPATH"):
            env.pop(key, None)
        conf_dir = self.root / "conf"
        conf_dir.mkdir()
        env.update(SPARK_HOME=str(self.spark_home), SPARK_CONF_DIR=str(conf_dir),
                   SPARK_LOCAL_IP="127.0.0.1", PYSPARK_PYTHON=sys.executable,
                   PYSPARK_DRIVER_PYTHON=sys.executable, PYTHONUNBUFFERED="1")
        return env

    def server_configuration(self):
        # Static/driver/storage configuration belongs here, never in a pipeline's session configuration.
        return {
            "spark.sql.extensions": "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.kryo.registrator": "org.apache.spark.HoodieSparkKryoRegistrar",
            "spark.sql.catalogImplementation": "in-memory",
            "spark.sql.warehouse.dir": (self.root / "warehouse").as_uri(),
            "spark.hadoop.fs.defaultFS": "file:///",
            "spark.local.dir": str(self.root / "spark-local"),
            "spark.driver.host": "127.0.0.1",
            "spark.driver.bindAddress": "127.0.0.1",
            "spark.connect.grpc.binding.address": "127.0.0.1",
            "spark.connect.grpc.binding.port": str(self.port),
            "spark.port.maxRetries": "0",
            "spark.ui.enabled": "false",
            "spark.hudi.test.run.id": self.root.name,
        }

    def wait_for_port(self):
        deadline = time.monotonic() + self.startup_timeout
        while time.monotonic() < deadline:
            if self.server.poll() is not None:
                raise RuntimeError(f"Connect server exited with {self.server.returncode}; see server.log")
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=0.5):
                    return
            except OSError:
                time.sleep(0.2)
        raise TimeoutError(f"Connect server startup exceeded {self.startup_timeout}s; see server.log")

    def run(self, command, name, timeout=None):
        log = self.logs / f"{name}.log"
        with owned_process(command, log, self.root, self.env) as process:
            wait_for_process(process, timeout or self.client_timeout, log, self.server)

    def client(self, script, name, timeout=None):
        self.run([sys.executable, str(script), name, self.remote, str(self.root)], name, timeout)

    def run_pipeline(self, spec, name="pipeline"):
        self.run([str(self.spark_home / "bin/spark-pipelines"), "run", "--remote", self.remote,
                  "--spec", str(spec)], name)

    def smoke(self):
        input_dir = self.root / "input"
        input_dir.mkdir()
        rows = [{"id": i, "ts": 100 + i, "value": f"value-{i}"} for i in range(1, 4)]
        (input_dir / "events.json").write_text("".join(json.dumps(row) + "\n" for row in rows))
        spec = {
            "name": "hudi_connect_smoke",
            "storage": (self.root / "checkpoints").as_uri(),
            "catalog": "spark_catalog",
            "database": "sdp_test",
            "libraries": [{"glob": {"include": "pipeline.py"}}],
            "configuration": {
                "spark.sql.shuffle.partitions": "2",
                "spark.sql.pipelines.maxFlowRetryAttempts": "0",
                "hudi.test.input": input_dir.as_uri(),
            },
        }
        (self.root / "pipeline.py").write_text((HERE / "pipeline.py").read_text())
        spec_path = self.root / "spark-pipeline.yml"
        # JSON is a YAML subset; keep harness lifecycle tests independent of third-party Python packages.
        spec_path.write_text(json.dumps(spec, indent=2) + "\n")
        (self.logs / spec_path.name).write_text(spec_path.read_text())
        self.run_pipeline(spec_path)
        self.client(HERE / "client.py", "verify")
        if not list((self.root / "checkpoints").rglob("offsets/0")):
            raise AssertionError("SDP did not create a streaming checkpoint under its isolated storage directory")


def positive_seconds(value):
    seconds = int(value)
    if seconds <= 0:
        raise argparse.ArgumentTypeError("timeout must be positive")
    return seconds


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", required=True, type=Path)
    parser.add_argument("--logs", type=Path, default=Path("target/spark-pipelines-logs"))
    parser.add_argument("--startup-timeout", type=positive_seconds, default=90)
    parser.add_argument("--client-timeout", type=positive_seconds, default=180)
    args = parser.parse_args()
    args.logs.mkdir(parents=True, exist_ok=True)
    logs = Path(tempfile.mkdtemp(prefix="run-", dir=args.logs.resolve()))
    print(f"Spark Connect SDP logs: {logs}", flush=True)
    # Let CI cancellation reach ExitStack, just like Ctrl-C; each child has its own process group.
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"Received signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    try:
        with ConnectHarness(args.bundle, logs, args.startup_timeout, args.client_timeout) as harness:
            harness.smoke()
    except (Exception, KeyboardInterrupt) as error:
        print(f"FAILED: {error}", file=sys.stderr)
        for log in sorted(logs.glob("*.log")):
            print(f"\n--- {log.name} (last 60 lines) ---", file=sys.stderr)
            with log.open(errors="replace") as output:
                print("".join(deque(output, maxlen=60)), file=sys.stderr)
        return 1
    print("PASS: spark-pipelines wrote the expected Hudi rows through Spark Connect; resources removed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
