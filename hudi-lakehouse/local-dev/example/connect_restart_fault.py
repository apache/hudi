# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Opt-in driver interruption experiment. Run only on a disposable endpoint."""

import argparse
from contextlib import ExitStack
import json
import queue
import re
import subprocess
import sys
import threading
import time
import uuid
from adbc_driver_manager import dbapi


def connect_ready(connect, port):
    """Transfer ownership only after the connection can execute a query."""
    with ExitStack() as stack:
        connection = connect(port)
        stack.callback(connection.close)
        cursor = connection.cursor()
        stack.callback(cursor.close)
        cursor.execute("SELECT 1")
        cursor.fetchall()
        stack.pop_all()
        return connection, cursor


def main():
    p = argparse.ArgumentParser(
        description="Interrupt a disposable Connect deployment at a Hudi commit stage."
    )
    p.add_argument(
        "--restart-endpoint",
        action="store_true",
        required=True,
        help="confirm that active sessions can be terminated",
    )
    p.add_argument("--stage", choices=("requested", "inflight"), required=True)
    p.add_argument(
        "--port", type=int, default=15039, help="existing localhost port-forward"
    )
    p.add_argument(
        "--reconnect-port",
        type=int,
        default=15040,
        help="unused localhost port for the restarted endpoint",
    )
    p.add_argument("--namespace", default="hudi-lakehouse")
    p.add_argument("--deployment", default="hudi-spark-connect")
    a = p.parse_args()
    if not (
        1 <= a.port <= 65535
        and 1 <= a.reconnect_port <= 65535
        and a.port != a.reconnect_port
    ):
        p.error("ports must be distinct valid TCP ports")
    ns = a.namespace
    deploy = a.deployment
    stage = a.stage
    db = "connect_restart_" + uuid.uuid4().hex
    table = db + ".orders"
    print("DB", db, "STAGE", stage, flush=True)

    def uri(port):
        return f"spark://127.0.0.1:{port}?api=connect&auth_type=none&tls=false"

    def connect(port):
        return dbapi.connect(
            driver="spark", db_kwargs={"uri": uri(port)}, autocommit=True
        )

    def kubectl(*a):
        return subprocess.run(
            ["kubectl", "-n", ns, *a],
            check=True,
            text=True,
            capture_output=True,
            timeout=360,
        ).stdout

    with connect(a.port) as c, c.cursor() as q:
        q.execute("CREATE DATABASE " + db)
        q.execute(
            f"CREATE TABLE {table} (id INT, ts BIGINT) USING hudi TBLPROPERTIES(type='cow',primaryKey='id',preCombineField='ts')"
        )
    deployment = json.loads(kubectl("get", "deployment", deploy, "-o", "json"))
    selector = ",".join(
        f"{key}={value}"
        for key, value in deployment["spec"]["selector"]["matchLabels"].items()
    )
    pods = json.loads(kubectl("get", "pods", "-l", selector, "-o", "json"))["items"]
    pods = [
        item
        for item in pods
        if item["status"].get("phase") == "Running"
        and not item["metadata"].get("deletionTimestamp")
    ]
    if len(pods) != 1:
        print("INCONCLUSIVE: expected one running driver; preserve", db, flush=True)
        sys.exit(2)
    pod = pods[0]["metadata"]["name"]
    print("POD", pod, flush=True)
    logs = subprocess.Popen(
        ["kubectl", "-n", ns, "logs", "-f", pod, "--tail=0"],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        bufsize=1,
    )
    lines = queue.Queue()

    def readlogs():
        for line in logs.stdout:
            lines.put(line)

    threading.Thread(target=readlogs, daemon=True).start()
    time.sleep(2)
    source = """from adbc_driver_manager import dbapi\nimport sys\nwith dbapi.connect(driver="spark",db_kwargs={"uri":sys.argv[1]},autocommit=True) as c,c.cursor() as q:\n q.execute(f"INSERT INTO {sys.argv[2]} SELECT CAST(id AS INT) AS id, CAST(id AS BIGINT) AS ts FROM range(100000)")\n"""
    writer = subprocess.Popen(
        [sys.executable, "-c", source, uri(a.port), table],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    instant = None
    deadline = time.monotonic() + 90
    try:
        while time.monotonic() < deadline:
            try:
                line = lines.get(timeout=1)
            except queue.Empty:
                if writer.poll() is not None:
                    print("Writer exited before targeted stage", writer.returncode)
                    break
                continue
            if stage == "requested":
                m = re.search(
                    r"Creating a new instant \[==>(\d+)__commit__REQUESTED\]", line
                )
            else:
                m = re.search(
                    re.escape(table.replace(".", ".db/"))
                    + r"/\.hoodie/timeline/(\d+)\.inflight",
                    line,
                )
            if m:
                instant = m.group(1)
                print("MATCH", line.strip(), flush=True)
                break
        if not instant:
            print("INCONCLUSIVE preserve", db, flush=True)
            sys.exit(2)
        print("DELETE DRIVER", flush=True)
        kubectl("delete", "pod", pod, "--force", "--grace-period=0", "--wait=true")
        kubectl("rollout", "status", "deployment/" + deploy, "--timeout=300s")
        try:
            writer.wait(timeout=30)
        except subprocess.TimeoutExpired:
            writer.terminate()
            writer.wait(timeout=10)
        print("Writer exit", writer.returncode, flush=True)
        fwd = subprocess.Popen(
            [
                "kubectl",
                "-n",
                ns,
                "port-forward",
                "svc/" + deploy,
                f"{a.reconnect_port}:15002",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            time.sleep(2)
            if fwd.poll() is not None:
                raise RuntimeError("reconnect port-forward failed; preserve " + db)
            for i in range(60):
                if fwd.poll() is not None:
                    raise RuntimeError("reconnect port-forward exited; preserve " + db)
                try:
                    c, q = connect_ready(connect, a.reconnect_port)
                    break
                except Exception:
                    time.sleep(2)
            else:
                raise RuntimeError("new server unavailable")
            try:
                q.execute(f"SELECT COUNT(*) FROM {table}")
                rows = q.fetchall()
                print("ROWS AFTER RESTART", rows, flush=True)
                if rows != [(0,)]:
                    print(
                        "INCONCLUSIVE: original write is not empty; preserve",
                        db,
                        flush=True,
                    )
                    sys.exit(2)
                q.execute(f"INSERT INTO {table} VALUES (-1, 999)")
                q.execute(f"SELECT id, ts FROM {table} WHERE id=-1")
                if q.fetchall() != [(-1, 999)]:
                    raise RuntimeError(
                        "recovery write returned unexpected rows; preserve " + db
                    )
                server = kubectl("logs", "deployment/" + deploy, "--tail=10000")
                matches = [
                    l
                    for l in server.splitlines()
                    if ("rollback" in l.lower() and (instant in l or table in l))
                ]
                print("ROLLBACK EVIDENCE", matches[:8], flush=True)
                if not any(
                    f"Rollback of Commits [{instant}] is complete" in line
                    for line in matches
                ):
                    print(
                        "INCONCLUSIVE: matching rollback completion not observed; preserve",
                        db,
                        flush=True,
                    )
                    sys.exit(2)
                q.execute(f"DROP TABLE IF EXISTS {table} PURGE")
                q.execute(f"DROP DATABASE IF EXISTS {db}")
                print("PASS recovered and cleaned", flush=True)
            finally:
                try:
                    q.close()
                finally:
                    c.close()
        finally:
            fwd.terminate()
            fwd.wait(timeout=10)
    finally:
        logs.terminate()
        logs.wait(timeout=10)
        if writer.poll() is None:
            writer.terminate()
            writer.wait(timeout=10)


if __name__ == "__main__":
    main()
