/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities.unstructured.embedding.streamer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.utilities.sources.blob.BlobFileNames;
import org.apache.hudi.utilities.sources.blob.BucketFolderFileSystem;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.FixtureFile;
import org.apache.hudi.utilities.unstructured.embedding.compute.DummyEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.prep.pdf.PdfFixtures;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Checks one embedding round over the unstructured data source on S3 against a recorded golden on a fixed PDF corpus, with the dummy provider
 * and the default chunking. The golden was recorded from the prep and compute writers at generator commit 0e29bf808e by
 * {@code golden/generate-golden.sh} (test resources), over the corpus {@link PdfFixtures} builds; this test checks the corpus
 * bytes against the SHA-256 the script recorded.
 *
 * <p>Rows are joined on (file, modality, chunk_id) and compared on the prepared text, its token count and aux. What differs by
 * design is not compared: the golden's objects were named {@code s3a://parity/<file>} and this round's are announced at
 * {@code <bucket>/corpus/<file>}, so source_path_sha (and every column derived from the path) differs; the dummy vector is a
 * function of (source_path_sha, chunk_id), so the vector is checked against that function on each side's own sha. The golden
 * holds only the columns compared here.
 */
@Tag("functional")
public class TestEmbeddingRoundGolden extends EmbeddingRoundTestBase {

  private static final String GOLDEN = "/unstructured/embedding/golden/";
  /** How the golden's corpus named its objects. */
  private static final String GOLDEN_PREFIX = "s3a://parity/";

  private static Map<String, byte[]> corpus() throws IOException {
    Map<String, byte[]> pdfs = new TreeMap<>();
    pdfs.put("short.pdf", PdfFixtures.text("Alpha beta gamma delta epsilon zeta.", "Eta theta iota kappa lambda mu."));
    pdfs.put("prose-3.pdf", PdfFixtures.prose(1L, 3, 12));
    pdfs.put("prose-6.pdf", PdfFixtures.prose(2L, 6, 24));
    pdfs.put("two-column.pdf", PdfFixtures.multiColumn("Left column about lakes and tables.", "Right column about vectors and models."));
    return pdfs;
  }

  private static String sha256(byte[] bytes) throws Exception {
    StringBuilder hex = new StringBuilder();
    for (byte b : MessageDigest.getInstance("SHA-256").digest(bytes)) {
      hex.append(String.format("%02x", b));
    }
    return hex.toString();
  }

  private static byte[] resource(String name) throws IOException {
    try (InputStream in = TestEmbeddingRoundGolden.class.getResourceAsStream(GOLDEN + name)) {
      assertNotNull(in, GOLDEN + name);
      return in.readAllBytes();
    }
  }

  @Test
  public void roundOverEventsSourceMatchesGolden() throws Exception {
    List<FixtureFile> files = new ArrayList<>();
    List<String> manifest = new ArrayList<>();
    Map<String, String> goldenShaByFile = new TreeMap<>();
    for (Map.Entry<String, byte[]> pdf : corpus().entrySet()) {
      files.add(object("corpus/" + pdf.getKey(), pdf.getValue()));
      manifest.add(sha256(pdf.getValue()) + "  " + pdf.getKey());
      goldenShaByFile.put(pdf.getKey(), BlobFileNames.sourcePathSha(GOLDEN_PREFIX + pdf.getKey()));
    }
    assertEquals(Arrays.asList(new String(resource("corpus.sha256.txt"), StandardCharsets.UTF_8).split("\n")), manifest,
        "the corpus is the one the golden was generated from");
    announce(files.toArray(new FixtureFile[0]));

    round(EmbeddingStreamers.jobProps(table(), objects().eventsPath(), BucketFolderFileSystem.SCHEME, INLINE_MAX_BYTES,
        EmbeddingTableConfig.MODE_PREP_AND_COMPUTE, withBalance()));

    String prepVersion = org.apache.hudi.common.table.HoodieTableMetaClient.builder().setBasePath(table())
        .setConf(org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf()).build()
        .getTableConfig().getProps().getProperty("hoodie.table.embedding.prep.text.logic.version");
    assertNotNull(prepVersion);
    // golden line: sha, modality, chunk, token count, aux, text, vector; this round's rows in the golden's coordinates
    TreeMap<String, String> lines = new TreeMap<>();
    for (Row row : rows(table()).values()) {
      String sha = row.getAs(EmbeddingTableSchema.SOURCE_PATH_SHA);
      String goldenSha = goldenShaByFile.get(row.<String>getAs(EmbeddingTableSchema.SOURCE_FILE_NAME));
      String modality = row.getAs(EmbeddingTableSchema.MODALITY);
      long chunk = row.getAs(EmbeddingTableSchema.CHUNK_ID);
      assertArrayEquals(DummyEmbeddingClient.vectorFor(sha, chunk, EmbeddingStreamers.MODEL_DIMENSIONS), vector(row),
          "every row carries the vector of its own key: " + row);
      Map<String, String> aux = new TreeMap<>(row.getJavaMap(row.fieldIndex(EmbeddingTableSchema.AUX)));
      assertEquals(prepVersion, aux.remove(EmbeddingTableSchema.AUX_PREP_LOGIC_VERSION));
      lines.put(goldenSha + "\t" + modality + "\t" + String.format("%06d", chunk), goldenSha + "\t" + modality + "\t" + chunk + "\t"
          + row.getAs(EmbeddingTableSchema.CHUNK_TOKEN_COUNT) + "\t" + JsonUtils.getObjectMapper().writeValueAsString(aux) + "\t"
          + JsonUtils.getObjectMapper().writeValueAsString(row.<String>getAs(EmbeddingTableSchema.PREPARED_TEXT_CHUNK)));
    }
    List<String> golden = new ArrayList<>();
    for (String line : new String(resource("golden.txt"), StandardCharsets.UTF_8).split("\n")) {
      String[] fields = line.split("\t");
      float[] goldenVector = new float[EmbeddingStreamers.MODEL_DIMENSIONS];
      String[] values = fields[6].split(",");
      for (int i = 0; i < goldenVector.length; i++) {
        goldenVector[i] = Float.parseFloat(values[i]);
      }
      assertArrayEquals(DummyEmbeddingClient.vectorFor(fields[0], Long.parseLong(fields[2]), EmbeddingStreamers.MODEL_DIMENSIONS),
          goldenVector, "the golden's vector is the dummy vector of its own key: " + line);
      golden.add(line.substring(0, line.lastIndexOf('\t')));
    }
    assertEquals(golden.size(), lines.size(), "rows");
    assertEquals(golden, new ArrayList<>(lines.values()));
  }

  private static TypedProperties withBalance() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.BALANCE_MIN_PARTITIONS.key(), "2");
    return props;
  }
}
