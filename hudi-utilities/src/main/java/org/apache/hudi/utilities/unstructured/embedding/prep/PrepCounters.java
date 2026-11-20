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

package org.apache.hudi.utilities.unstructured.embedding.prep;

import org.apache.spark.SparkContext;
import org.apache.spark.util.LongAccumulator;

import java.io.Serializable;

/**
 * Data-prep counters, summed across Spark tasks.
 */
public class PrepCounters implements Serializable {

  private static final long serialVersionUID = 1L;

  private final LongAccumulator filesOk;
  private final LongAccumulator filesFailed;
  private final LongAccumulator chunks;
  private final LongAccumulator filesNoPreprocessor;
  private final LongAccumulator filesNoText;

  public PrepCounters(LongAccumulator filesOk, LongAccumulator filesFailed, LongAccumulator chunks,
                      LongAccumulator filesNoPreprocessor, LongAccumulator filesNoText) {
    this.filesOk = filesOk;
    this.filesFailed = filesFailed;
    this.chunks = chunks;
    this.filesNoPreprocessor = filesNoPreprocessor;
    this.filesNoText = filesNoText;
  }

  public static PrepCounters register(SparkContext sc) {
    return new PrepCounters(sc.longAccumulator("embedding.prep.filesOk"),
        sc.longAccumulator("embedding.prep.filesFailed"), sc.longAccumulator("embedding.prep.chunks"),
        sc.longAccumulator("embedding.prep.filesNoPreprocessor"),
        sc.longAccumulator("embedding.prep.filesNoText"));
  }

  /** Counters that are not registered with Spark (unit tests, local use). */
  public static PrepCounters local() {
    return new PrepCounters(new LongAccumulator(), new LongAccumulator(), new LongAccumulator(),
        new LongAccumulator(), new LongAccumulator());
  }

  public LongAccumulator getFilesOk() {
    return filesOk;
  }

  public LongAccumulator getFilesFailed() {
    return filesFailed;
  }

  public LongAccumulator getChunks() {
    return chunks;
  }

  /** Files whose extension no preprocessor is mapped to (an empty side output, not a failure). */
  public LongAccumulator getFilesNoPreprocessor() {
    return filesNoPreprocessor;
  }

  /** Files whose preprocessor finished without error and without any chunk (an empty side output, not a failure). */
  public LongAccumulator getFilesNoText() {
    return filesNoText;
  }
}
