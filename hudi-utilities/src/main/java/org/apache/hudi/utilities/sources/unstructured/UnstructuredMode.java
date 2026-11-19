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

package org.apache.hudi.utilities.sources.unstructured;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.sources.blob.BlobQuarantineTransformer;
import org.apache.hudi.utilities.sources.blob.BlobTableSchema;
import org.apache.hudi.utilities.transform.Transformer;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * The job of an unstructured data source ({@link UnstructuredSourceConfig#MODE}): which table it writes, the transformer the
 * streamer runs in every round, the transformer it refuses, and the checks of the job's own properties that belong to that job.
 *
 * <p>The blob table's physical write settings are job properties; the effective inline threshold, codec and bloom switch are
 * also pinned in the table's properties at creation ({@link org.apache.hudi.utilities.sources.blob.BlobPhysicalPins}) and an
 * ingestion job whose values differ is refused. Only the ingestion job is checked against those pins: a table service run as a
 * separate job (compaction, clustering) is not, and rewrites the files with whatever settings it carries. Every job that
 * rewrites its files, including a separate compaction or clustering job, must therefore carry the ingestion job's physical
 * settings:
 * <ul>
 *   <li>{@code hoodie.parquet.compression.codec=uncompressed} and {@code hoodie.parquet.bloom.filter.enabled=false}, the blob
 *       job's defaults. Bloom is off under the memory-pressure write defaults (ENG-50740). An explicit user value is kept;
 *       carry any explicit ingestion overrides into the table-service job as well;</li>
 *   <li>{@code hoodie.parquet.internal.column.dictionary.enabled#blob.data=false}, and
 *       {@code hoodie.parquet.internal.column.statistics.enabled#blob.data=false} on Parquet 1.14 or later, where the per-column
 *       statistics switch is supported. They are Hudi-internal keys: an ordinary writer ignores Parquet's generic keys, as
 *       upstream.</li>
 * </ul>
 * Without these properties, a table-service job rewrites files with Hudi's default physical settings.
 */
public enum UnstructuredMode {

  /** Writes the blob table, through {@link BlobQuarantineTransformer}. */
  BLOB("blob", BlobTableSchema.KEY_COLUMNS, BlobQuarantineTransformer.class);

  private final String configValue;
  private final List<String> keyColumns;
  private final Class<? extends Transformer> requiredTransformer;

  UnstructuredMode(String configValue, List<String> keyColumns, Class<? extends Transformer> requiredTransformer) {
    this.configValue = configValue;
    this.keyColumns = keyColumns;
    this.requiredTransformer = requiredTransformer;
  }

  /**
   * The job {@link UnstructuredSourceConfig#MODE} names.
   *
   * @throws IllegalArgumentException naming the key and its values when it is unset or another value
   */
  public static UnstructuredMode from(TypedProperties props) {
    return UnstructuredSourceKeys.read(props, UnstructuredSourceConfig.MODE, values(), UnstructuredMode::configValue);
  }

  /** The value of {@link UnstructuredSourceConfig#MODE} that names this job. */
  public String configValue() {
    return configValue;
  }

  /** The record key columns of the job's table, in order. */
  public List<String> keyColumns() {
    return keyColumns;
  }

  /** The transformer the streamer runs in every round of this job. */
  public Class<? extends Transformer> requiredTransformer() {
    return requiredTransformer;
  }

  /** The transformers this job refuses in its chain: none, while it is the only job. */
  public Set<Class<? extends Transformer>> excludedTransformers() {
    return Collections.emptySet();
  }

  /**
   * Checks the job's own properties that only this job reads, and changes none of them. Refusals name {@code sourceName}.
   *
   * @throws IllegalArgumentException when a setting has a value this job cannot run with
   */
  public void validate(TypedProperties props, String sourceName) {
    switch (this) {
      case BLOB:
        break;
      default:
        throw new UnsupportedOperationException("Unknown unstructured mode " + this);
    }
  }
}
