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

"""Freshness fixture: a separate, short-lived Spark JVM, NOT a Connect client.

Launches a local-mode writer pod with the endpoint image, catalog configuration,
and Secret references. Needs kubectl permission to create/delete pods/configmaps.
Only touches the acceptance test's uniquely named freshness_orders table.
"""
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time


def write(database, action):
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("connect-external-writer").getOrCreate()
    try:
        table = f"{database}.freshness_orders"
        if action == "commit-schema":
            spark.sql(f"ALTER TABLE {table} ADD COLUMNS (note STRING)").collect()
            spark.sql(f"INSERT INTO {table} VALUES (2, 200, 2, 'external')").collect()
        else:
            spark.sql(f"DROP TABLE {table}").collect()
            spark.sql(f"""CREATE TABLE {table} (id INT, replacement STRING, ts BIGINT)
                USING hudi TBLPROPERTIES (type = 'cow', primaryKey = 'id', preCombineField = 'ts')""").collect()
            spark.sql(f"INSERT INTO {table} VALUES (9, 'recreated', 3)").collect()
        print(f"EXTERNAL_WRITE_OK {action}", flush=True)
    finally:
        spark.stop()


def writer_pod(template, name, database, action):
    """Reuse only connection config; never reuse the endpoint's labels/probes."""
    container = next(c for c in template["containers"] if c["name"] == "spark-connect")
    env = [e for e in container["env"] if e["name"] == "HUDI_CONF_DIR" or e["name"].startswith("AWS_")]
    mounts = [m for m in container["volumeMounts"] if m["name"] in ("config", "hudi-config", "scratch")]
    volumes = [v for v in template["volumes"] if v["name"] in ("config", "hudi-config", "scratch")]
    return {
        "apiVersion": "v1", "kind": "Pod", "metadata": {"name": name},
        "spec": {
            "restartPolicy": "Never", "automountServiceAccountToken": False,
            "activeDeadlineSeconds": 360,
            "securityContext": template["securityContext"],
            "imagePullSecrets": template.get("imagePullSecrets", []),
            "containers": [{
                "name": "writer", "image": container["image"],
                "imagePullPolicy": container["imagePullPolicy"],
                "securityContext": container["securityContext"],
                "env": env,
                "command": ["/opt/spark/bin/spark-submit"],
                "args": ["--master", "local[1]", "--driver-memory", "512m",
                         "--conf", "spark.ui.enabled=false", "--conf", "spark.sql.shuffle.partitions=1",
                         "/smoke/connect_external_writer.py", "write", database, action],
                "resources": {"requests": {"cpu": "250m", "memory": "1Gi"},
                              "limits": {"memory": "1536Mi"}},
                "volumeMounts": mounts + [{"name": "script", "mountPath": "/smoke", "readOnly": True}],
            }],
            "volumes": volumes + [{"name": "script", "configMap": {"name": name}}],
        },
    }


def launch(database, action):
    namespace = os.environ.get("NAMESPACE", "hudi-lakehouse")
    deployment = os.environ.get("DEPLOYMENT", "hudi-spark-connect")
    name = database.replace("_", "-") + "-writer"
    if len(name) > 63:
        raise ValueError("Test database name is too long for a writer pod")
    prefix = ["kubectl", "-n", namespace]

    def kubectl(*args, **kwargs):
        return subprocess.run([*prefix, *args], check=True, text=True, timeout=30, **kwargs)

    template = json.loads(kubectl("get", "deployment", deployment, "-o", "json",
                                  capture_output=True).stdout)["spec"]["template"]["spec"]
    config_created = pod_created = False
    try:
        # create, not apply: a name collision must not authorize deleting others' resources.
        kubectl("create", "configmap", name, f"--from-file=connect_external_writer.py={Path(__file__).resolve()}")
        config_created = True
        kubectl("create", "-f", "-", input=json.dumps(writer_pod(template, name, database, action)))
        pod_created = True
        deadline = time.monotonic() + 390
        while time.monotonic() < deadline:
            phase = kubectl("get", "pod", name, "-o", "jsonpath={.status.phase}", capture_output=True).stdout
            if phase in ("Succeeded", "Failed"):
                logs = kubectl("logs", name, capture_output=True).stdout
                print(logs, flush=True)
                if phase != "Succeeded" or f"EXTERNAL_WRITE_OK {action}" not in logs:
                    raise RuntimeError(f"External writer failed: {phase}")
                return
            time.sleep(2)
        kubectl("describe", "pod", name)
        raise TimeoutError("External writer did not finish")
    finally:
        # The Pod and ConfigMap are private to this run; table cleanup belongs to the caller.
        try:
            if pod_created:
                kubectl("delete", "pod", name, "--ignore-not-found", "--wait=true", "--timeout=20s")
        finally:
            if config_created:
                kubectl("delete", "configmap", name, "--ignore-not-found")


if __name__ == "__main__":
    args = sys.argv[1:]
    in_writer = args[:1] == ["write"]
    if in_writer:
        args = args[1:]
    if len(args) != 2 or not re.fullmatch(r"connect_smoke_[a-z0-9_]+", args[0]) or args[1] not in ("commit-schema", "recreate"):
        raise SystemExit("Usage: connect_external_writer.py [write] connect_smoke_<unique> commit-schema|recreate")
    (write if in_writer else launch)(*args)
