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

"""Offline chart contract tests. Require Python + PyYAML, Helm, and JDK 11+."""
import base64
import json
import pathlib
import subprocess
import tempfile
import unittest

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
CHART = ROOT / "charts/hudi-spark-connect"
LOCAL = CHART / "values-local-dev.yaml"


def render(overrides=None, *, local=True, success=True, extra_args=()):
    args = ["helm", "template", "test", str(CHART)]
    if local:
        args += ["-f", str(LOCAL)]
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml") as config:
        yaml.safe_dump(overrides or {}, config)
        config.flush()
        proc = subprocess.run(args + ["-f", config.name, *extra_args], capture_output=True, text=True)
    if success:
        if proc.returncode:
            raise AssertionError(proc.stderr)
        return {doc["kind"]: doc for doc in yaml.safe_load_all(proc.stdout) if doc}
    if not proc.returncode:
        raise AssertionError("Expected invalid configuration to fail")
    return proc.stderr


class ConnectChartTest(unittest.TestCase):
    def test_local_mode_and_persistent_catalog(self):
        docs = render()
        config = docs["ConfigMap"]["data"]["spark-defaults.conf"]
        self.assertIn("spark.master local[2]", config)
        self.assertIn("spark.connect.grpc.arrow.maxBatchSize 1m", config)
        self.assertIn("spark.hadoop.hive.metastore.uris thrift://hive-metastore.", config)
        self.assertIn("spark.sql.warehouse.dir s3a://warehouse/connect", config)
        self.assertIn("spark.sql.runSQLOnFiles false", config)
        self.assertIn("org.apache.spark.sql.hudi.catalog.HoodieCatalog", config)
        self.assertIn("HoodieSparkSessionExtension", config)
        self.assertEqual(docs["Deployment"]["spec"]["replicas"], 1)
        self.assertEqual(docs["Deployment"]["spec"]["strategy"]["type"], "Recreate")
        self.assertEqual(docs["Service"]["spec"]["type"], "ClusterIP")
        pod = docs["Deployment"]["spec"]["template"]["spec"]
        self.assertFalse(pod["automountServiceAccountToken"])
        self.assertTrue(pod["securityContext"]["runAsNonRoot"])

    def test_scratch_volume_size_limit(self):
        for size, expected in (("", {}), ("20Gi", {"sizeLimit": "20Gi"})):
            with self.subTest(size=size):
                pod = render({"scratch": {"sizeLimit": size}})["Deployment"]["spec"]["template"]["spec"]
                volume = next(v for v in pod["volumes"] if v["name"] == "scratch")
                self.assertEqual(volume["emptyDir"], expected)
                self.assertIn({"name": "scratch", "mountPath": "/tmp"}, pod["containers"][0]["volumeMounts"])

    def test_credentials_only_reference_existing_secret(self):
        docs = render({"storage": {"s3": {"sessionTokenKey": "token"}}})
        self.assertNotIn("Secret", docs)
        config = docs["ConfigMap"]["data"]["spark-defaults.conf"]
        self.assertNotIn("password", config)
        self.assertNotIn("access.key", config)
        env = {e["name"]: e for e in docs["Deployment"]["spec"]["template"]["spec"]["containers"][0]["env"]}
        self.assertEqual(env["AWS_ACCESS_KEY_ID"]["valueFrom"]["secretKeyRef"],
                         {"name": "spark-connect-s3", "key": "accessKey"})
        self.assertEqual(env["AWS_SESSION_TOKEN"]["valueFrom"]["secretKeyRef"]["key"], "token")

    def test_external_s3_and_default_credentials(self):
        docs = render({"catalog": {"metastoreUri": "thrift://hms.example:9083",
                                   "warehouse": "s3a://example/warehouse"}}, local=False)
        env = docs["Deployment"]["spec"]["template"]["spec"]["containers"][0]["env"]
        self.assertNotIn("AWS_ACCESS_KEY_ID", [item["name"] for item in env])
        self.assertNotIn("spark.hadoop.fs.s3a.endpoint ", docs["ConfigMap"]["data"]["spark-defaults.conf"])

    def test_required_catalog(self):
        self.assertIn("catalog.metastoreUri is required", render(local=False, success=False))
        self.assertIn("catalog.warehouse is required", render(
            {"catalog": {"metastoreUri": "thrift://hms:9083"}}, local=False, success=False))

    def test_config_change_rolls_deployment(self):
        old = render()
        new = render({"spark": {"extraConf": {"spark.sql.session.timeZone": "UTC"}}})
        def checksum(docs):
            return docs["Deployment"]["spec"]["template"]["metadata"]["annotations"]["checksum/config"]
        self.assertNotEqual(checksum(old), checksum(new))
        new_hudi = render({"hudi": {"defaults": {"hoodie.clean.automatic": False}}})
        self.assertNotEqual(checksum(old), checksum(new_hudi))

    def test_hudi_defaults_mounted_and_local_version_not_imposed_globally(self):
        docs = render()
        properties = docs["ConfigMap"]["data"]["hudi-defaults.conf"]
        self.assertIn("hoodie.write.table.version=8", properties)
        self.assertIn("hoodie.write.auto.upgrade=false", properties)
        pod = docs["Deployment"]["spec"]["template"]["spec"]
        container = pod["containers"][0]
        self.assertIn({"name": "HUDI_CONF_DIR", "value": "/opt/hudi/conf"}, container["env"])
        self.assertIn({"name": "config", "mountPath": "/opt/hudi/conf", "readOnly": True},
                      container["volumeMounts"])
        volume = next(v for v in pod["volumes"] if v["name"] == "config")
        self.assertEqual(volume["configMap"]["name"], docs["ConfigMap"]["metadata"]["name"])
        generic = render({"catalog": {"metastoreUri": "thrift://hms:9083", "warehouse": "s3a://bucket/"}}, local=False)
        self.assertFalse(generic["ConfigMap"]["data"]["hudi-defaults.conf"].strip())

    def test_kubernetes_master_uses_existing_service_account_and_executor_config(self):
        docs = render({"spark": {"master": "k8s://https://kubernetes.default.svc:443", "extraConf": {
            "spark.executor.instances": 2, "spark.executor.memory": "1g"}},
            "serviceAccount": {"name": "spark", "automountToken": True},
            "imagePullSecrets": [{"name": "registry-one"}, {"name": "registry-two"}],
            "storage": {"s3": {"sessionTokenKey": "token"}}})
        conf = docs["ConfigMap"]["data"]["spark-defaults.conf"]
        for setting in ("spark.master k8s://https://kubernetes.default.svc:443",
                        "spark.executor.instances 2", "spark.executor.memory 1g",
                        "spark.kubernetes.namespace default",
                        "spark.kubernetes.container.image.pullSecrets registry-one,registry-two",
                        "spark.kubernetes.executor.container.image hudi-lakehouse-spark-connect:4.1.3-hudi1.2.0",
                        "spark.kubernetes.executor.secretKeyRef.AWS_SESSION_TOKEN spark-connect-s3:token",
                        "spark.kubernetes.executor.secretKeyRef.AWS_ACCESS_KEY_ID spark-connect-s3:accessKey",
                        "spark.kubernetes.executor.secretKeyRef.AWS_SECRET_ACCESS_KEY spark-connect-s3:secretKey"):
            self.assertIn(setting, conf)
        pod = docs["Deployment"]["spec"]["template"]["spec"]
        self.assertEqual(pod["serviceAccountName"], "spark")
        self.assertTrue(pod["automountServiceAccountToken"])
        self.assertIn("spark.driver.host=$(POD_IP)", pod["containers"][0]["args"])
        self.assertIn("spark.kubernetes.driver.pod.name=$(POD_NAME)", pod["containers"][0]["args"])
        # Installing the chart must not grant permissions implicitly.
        self.assertNotIn("ClusterRole", docs)
        self.assertNotIn("RoleBinding", docs)

    def test_master_override_and_executor_image_override(self):
        self.assertIn("spark.master local[4]", render({"spark": {"cores": 4}})["ConfigMap"]["data"]["spark-defaults.conf"])
        conf = render({"spark": {"master": "k8s://https://api:443", "extraConf": {
            "spark.kubernetes.executor.container.image": "custom-executor:v1",
            "spark.kubernetes.container.image.pullSecrets": "executor-only"}},
            "imagePullSecrets": [{"name": "driver-only"}],
            "serviceAccount": {"name": "spark", "automountToken": True}})["ConfigMap"]["data"]["spark-defaults.conf"]
        self.assertIn("spark.kubernetes.executor.container.image custom-executor:v1", conf)
        self.assertIn("spark.kubernetes.container.image.pullSecrets executor-only", conf)

    def test_kubernetes_driver_identity_cannot_be_overridden(self):
        for key in ("spark.driver.host", "spark.kubernetes.driver.pod.name"):
            with self.subTest(key=key):
                error = render({"spark": {"master": "k8s://https://api:443", "extraConf": {key: "ignored"}},
                                "serviceAccount": {"name": "spark", "automountToken": True}}, success=False)
                self.assertIn(f"{key} is managed by the chart in Kubernetes mode", error)

    def test_port_and_names_match(self):
        docs = render({"fullnameOverride": "custom-connect", "service": {"port": 15012}})
        pod = docs["Deployment"]["spec"]["template"]
        self.assertEqual(docs["Service"]["spec"]["selector"], pod["metadata"]["labels"])
        self.assertEqual(docs["Service"]["spec"]["ports"][0]["port"], 15012)
        self.assertEqual(pod["spec"]["containers"][0]["ports"][0]["containerPort"], 15002)
        self.assertEqual(pod["spec"]["volumes"][0]["configMap"]["name"], docs["ConfigMap"]["metadata"]["name"])

    def test_invalid_values_rejected(self):
        invalid = [
            {"scratch": {"sizeLimit": 20}},
            {"spark": {"cores": 0}},
            {"spark": {"driverMemory": "oops"}},
            {"service": {"port": 65536}},
            {"service": {"type": "LoadBalancer"}},
            {"catalog": {"warehouse": "file:/tmp/warehouse"}},
            {"spark": {"extraConf": {"spark.master": "k8s://other"}}},
            {"spark": {"extraConf": {"spark.foo": "bar\nspark.master local[8]"}}},
            {"spark": {"extraConf": {"spark.foo=bar": "oops"}}},
            {"spark": {"master": "local[0]"}},
            {"spark": {"master": "spark://standalone:7077"}},
            {"spark": {"master": "k8s://https://api:443"}},
            {"spark": {"master": "k8s://https://api:443"}, "serviceAccount": {"name": "spark"}},
            {"hudi": {"defaults": {"not.hoodie": "value"}}},
            {"hudi": {"defaults": {"hoodie.test": "value\ninjected=true"}}},
            {"hudi": {"defaults": {"hoodie.test": ["not", "scalar"]}}},
            {"hudi": {"defaults": {"hoodie.test": " leading"}}},
            {"hudi": {"defaults": {"hoodie.test": "trailing "}}},
            {"hudi": {"defaults": {"hoodie.test": "two  spaces"}}},
            {"hudi": {"defaults": {"hoodie.test": "a\tb"}}},
        ]
        for values in invalid:
            with self.subTest(values=values):
                render(values, success=False)

    def test_extra_conf_backslashes_survive_java_properties_loading(self):
        values = {
            "spark.sql.redaction.options.regex": r"\d+\s+\w+",
            "spark.test.trailingBackslash": "path\\",
            "spark.test.literalUnicode": r"\u0041",
            "spark.test.whitespace": "  leading \t tab \f formfeed ",
        }
        config = render({"spark": {"extraConf": values}})["ConfigMap"]["data"]["spark-defaults.conf"]
        self.assert_java_properties(config, values)

    def test_hudi_defaults_keep_literal_backslashes_and_decimal_numbers(self):
        values = {"hoodie.test.regex": r"(?i)\b(password|token)\b", "hoodie.test.space": "one space",
                  "hoodie.test.literalUnicode": r"\u0041", "hoodie.test.trailing": "path\\",
                  "hoodie.test.empty": "", "hoodie.test.number": 10485760, "hoodie.test.flag": False}
        config = render({"hudi": {"defaults": values}})["ConfigMap"]["data"]["hudi-defaults.conf"]
        values.update({"hoodie.test.number": "10485760", "hoodie.test.flag": "false"})
        # DFSPropertiesConfiguration splits at the first '=' and keeps backslashes
        # literally, unlike Spark's java.util.Properties reader tested below.
        properties = dict(line.split("=", 1) for line in config.splitlines())
        for key, value in values.items():
            self.assertEqual(properties[key], value)

    def assert_java_properties(self, config, values):
        # Use Spark's actual properties-file parser, not a Python approximation.
        with tempfile.TemporaryDirectory() as directory:
            source = pathlib.Path(directory) / "ReadProperties.java"
            source.write_text("""
                import java.io.InputStreamReader;
                import java.nio.charset.StandardCharsets;
                import java.util.Base64;
                import java.util.Properties;
                class ReadProperties {
                    public static void main(String[] args) throws Exception {
                        Properties properties = new Properties();
                        properties.load(new InputStreamReader(System.in, StandardCharsets.UTF_8));
                        for (String key : args) {
                            System.out.println(Base64.getEncoder().encodeToString(
                                properties.getProperty(key).getBytes(StandardCharsets.UTF_8)));
                        }
                    }
                }
            """)
            result = subprocess.run(["java", str(source), *values], input=config,
                                    capture_output=True, text=True, check=True)
        self.assertEqual([base64.b64decode(value).decode("utf-8") for value in result.stdout.splitlines()],
                         list(values.values()))

    def test_numeric_properties_use_decimal_integer_notation(self):
        values = {
            "spark.sql.autoBroadcastJoinThreshold": 10485760,
            "spark.hadoop.fs.s3a.multipart.size": 1073741824,
            "spark.test.largeInteger": 2 ** 60,
            "spark.test.negativeInteger": -10485760,
            "spark.test.zero": 0,
        }
        sources = {
            "values.yaml": ({"spark": {"extraConf": values}}, ()),
            "--set-json": ({}, ("--set-json", "spark.extraConf=" + json.dumps(values))),
            "--set": ({}, tuple(arg for key, value in values.items()
                                for arg in ("--set", "spark.extraConf." + key.replace(".", r"\.")
                                            + "=" + str(value)))),
        }
        for source, (overrides, extra_args) in sources.items():
            with self.subTest(source=source):
                config = render(overrides, extra_args=extra_args)["ConfigMap"]["data"]["spark-defaults.conf"]
                properties = dict(line.split(None, 1) for line in config.splitlines())
                for key, value in values.items():
                    self.assertEqual(properties[key], str(value))

    def test_noninteger_property_values_are_preserved(self):
        values = {"spark.test.fraction": 0.125, "spark.test.enabled": False,
                  "spark.test.literal": "1.048576e+07", "spark.test.exactInteger": "9007199254740993"}
        config = render({"spark": {"extraConf": values}})["ConfigMap"]["data"]["spark-defaults.conf"]
        properties = dict(line.split(None, 1) for line in config.splitlines())
        self.assertEqual(properties["spark.test.fraction"], "0.125")
        self.assertEqual(properties["spark.test.enabled"], "false")
        self.assertEqual(properties["spark.test.literal"], "1.048576e+07")
        self.assertEqual(properties["spark.test.exactInteger"], "9007199254740993")

    def test_pinned_image_matches_chart(self):
        docs = render()
        image = docs["Deployment"]["spec"]["template"]["spec"]["containers"][0]["image"]
        self.assertEqual(image, "hudi-lakehouse-spark-connect:4.1.3-hudi1.2.0")
        dockerfile = (ROOT / "images/spark-connect/Dockerfile").read_text()
        self.assertIn("hudi-spark4.1-bundle_2.13/1.2.0/", dockerfile)
        self.assertIn("apache/spark:4.1.3-scala2.13-java17-python3-ubuntu", dockerfile)


if __name__ == "__main__":
    unittest.main()
