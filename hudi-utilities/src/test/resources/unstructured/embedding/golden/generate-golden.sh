#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Generates the golden of TestEmbeddingRoundGolden by running the prep and compute writers of <generator commit> over the fixed
# PDF corpus.
#
# Usage: generate-golden.sh <hudi checkout> <scratch dir> [generator commit, default 0e29bf808e]
# Needs JAVA_HOME (Java 11) and Maven with the caller's own settings and repository.
#
# 1. adds a git worktree of <generator commit> under <scratch dir> (removed again on exit);
# 2. writes the generator test below into it: it writes the fixed PDF corpus (PdfFixtures of that commit), a blob table holding
#    it, runs that commit's prep writer then its compute writer (dummy provider, 8 dimensions, default chunking), and writes one
#    line per embedding row: source_path_sha, modality, chunk_id, chunk_token_count, aux (JSON), prepared text (JSON), vector;
# 3. runs it with Maven (Spark 3.5, Java 11, -am) and writes next to this script golden.txt and corpus.sha256.txt
#    (the SHA-256 of each corpus PDF: the test builds the same PDFs with PdfFixtures and checks them against it).
set -euo pipefail
REPO="$(cd "$1" && pwd)"; SCRATCH="$2"; GENERATOR_COMMIT="${3:-0e29bf808e7ea0f101bc8b12184879c6e966feff}"
HERE="$(cd "$(dirname "$0")" && pwd)"
WT="$SCRATCH/golden-wt"
OUT="$SCRATCH/golden-out"
export JAVA_HOME="${JAVA_HOME:?set JAVA_HOME to a Java 11 JDK}"
rm -rf "${SCRATCH:?}/golden-out"
mkdir -p "$SCRATCH" "$OUT"
git -C "$REPO" worktree remove --force "$WT" 2>/dev/null || true
git -C "$REPO" worktree add --detach "$WT" "$GENERATOR_COMMIT"
trap 'git -C "$REPO" worktree remove --force "$WT"' EXIT
GEN="$WT/hudi-utilities/src/test/java/org/apache/hudi/utilities/unstructured/embedding/EmbeddingGoldenGenerator.java"
cat > "$GEN" <<'EOF'
package org.apache.hudi.utilities.unstructured.embedding;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.config.HoodieIncrSourceConfig;
import org.apache.hudi.utilities.unstructured.embedding.BlobTableFixtureWriter.FixtureFile;
import org.apache.hudi.utilities.unstructured.embedding.prep.pdf.PdfFixtures;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class EmbeddingGoldenGenerator extends SparkClientFunctionalTestHarness {

  @Test
  public void generate() throws Exception {
    Path out = Paths.get(System.getenv("GOLDEN_OUT"));
    Path corpus = out.resolve("corpus");
    Files.createDirectories(corpus);
    Map<String, byte[]> pdfs = new TreeMap<>();
    pdfs.put("short.pdf", PdfFixtures.text("Alpha beta gamma delta epsilon zeta.", "Eta theta iota kappa lambda mu."));
    pdfs.put("prose-3.pdf", PdfFixtures.prose(1L, 3, 12));
    pdfs.put("prose-6.pdf", PdfFixtures.prose(2L, 6, 24));
    pdfs.put("two-column.pdf", PdfFixtures.multiColumn("Left column about lakes and tables.", "Right column about vectors and models."));
    List<FixtureFile> files = new ArrayList<>();
    for (Map.Entry<String, byte[]> pdf : pdfs.entrySet()) {
      Files.write(corpus.resolve(pdf.getKey()), pdf.getValue());
      // Key encoding v1: scheme-free UTF-8 bucket/key, with no path normalization.
      byte[] digest = java.security.MessageDigest.getInstance("SHA-256")
          .digest(("parity/" + pdf.getKey()).getBytes(StandardCharsets.UTF_8));
      StringBuilder hex = new StringBuilder();
      for (byte value : digest) {
        hex.append(String.format("%02x", value));
      }
      files.add(new FixtureFile("s3a://parity/" + pdf.getKey(), pdf.getValue(), hex.substring(0, 32)));
    }
    String blob = basePath() + "/blob";
    String table = basePath() + "/emb";
    BlobTableFixtureWriter.writeCommit(spark(), blob, files, 1L << 20);
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.SOURCE_TABLE_PATH.key(), blob);
    props.setProperty(HoodieWriteConfig.BASE_PATH.key(), table);
    props.setProperty(HoodieIncrSourceConfig.MISSING_CHECKPOINT_STRATEGY.key(), "READ_UPTO_LATEST_COMMIT");
    props.setProperty(EmbeddingTableConfig.MODELS.key(), "m1");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m1" + EmbeddingTableConfig.MODEL_DIMENSIONS_SUFFIX, "8");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m1" + EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX, "dummy");
    props.setProperty(EmbeddingTableConfig.BALANCE_NUM_PARTITIONS.key(), "2");
    props.setProperty("hoodie.upsert.shuffle.parallelism", "2");
    props.setProperty("hoodie.insert.shuffle.parallelism", "2");
    EmbeddingStreamers.prep(spark(), props);
    EmbeddingStreamers.compute(spark(), props, "m1");
    TreeMap<String, String> lines = new TreeMap<>();
    for (Row row : spark().read().format("hudi").load(table).collectAsList()) {
      String sha = row.getAs("source_path_sha");
      String modality = row.getAs("modality");
      long chunk = row.getAs("chunk_id");
      Map<String, String> aux = new TreeMap<>(row.getJavaMap(row.fieldIndex("aux")));
      List<Float> vector = row.getList(row.fieldIndex("emb_m1"));
      StringBuilder csv = new StringBuilder();
      for (Float value : vector) {
        csv.append(csv.length() == 0 ? "" : ",").append(value);
      }
      String key = sha + "\t" + modality + "\t" + String.format("%06d", chunk);
      lines.put(key, sha + "\t" + modality + "\t" + chunk + "\t" + row.getAs("chunk_token_count") + "\t"
          + JsonUtils.getObjectMapper().writeValueAsString(aux) + "\t"
          + JsonUtils.getObjectMapper().writeValueAsString(row.<String>getAs("prepared_text_chunk")) + "\t" + csv);
    }
    Files.write(out.resolve("golden.tsv"), (String.join("\n", lines.values()) + "\n").getBytes(StandardCharsets.UTF_8));
  }
}
EOF
unset SPARK_HOME
export PATH="$JAVA_HOME/bin:$PATH"
export GOLDEN_OUT="$OUT"
(cd "$WT" && mvn -B -ntp -Dscala-2.12 -Dspark3.5 -Dflink2.1 -pl hudi-utilities -am -T 2 test -Dtest=EmbeddingGoldenGenerator \
  -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false -DwildcardSuites=none -Dcheckstyle.skip=true -Drat.skip=true \
  -Dscalastyle.skip=true -Djacoco.skip=true)
cp "$OUT/golden.tsv" "$HERE/golden.txt"
(cd "$OUT/corpus" && sha256sum -- *.pdf) > "$HERE/corpus.sha256.txt"
echo "generator commit: $(git -C "$WT" rev-parse HEAD)"
