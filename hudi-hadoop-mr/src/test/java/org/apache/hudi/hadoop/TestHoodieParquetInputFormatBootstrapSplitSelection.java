/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.hadoop;

import org.apache.hudi.common.util.Option;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Which of a bootstrap split's two files backs the read, per projection.
 *
 * <p>A bootstrap split carries the skeleton file as its own path - inside the table root - and the external
 * source file separately, outside it. Handing Hive a path outside the table root breaks its vectorized
 * parquet reader, which derives partition values by looking the split path up in {@code pathToPartitionInfo}
 * and fails with {@code cannot find dir = [...] in pathToPartitionInfo: [...]} (HUDI-5526, #15676). Hive 2
 * never vectorized this path, which is why the same query worked there.
 *
 * <p>This is the first coverage of that selection: nothing in the tree referenced
 * {@code BootstrapBaseFileSplit} or the reader built from it.
 */
class TestHoodieParquetInputFormatBootstrapSplitSelection {

  private static final Path SKELETON = new Path("s3://bucket/hudi-table/tbl/event_type=two/skeleton.parquet");
  private static final Path EXTERNAL = new Path("s3://bucket/source-tables/tbl/event_type=two/part-0.parquet");

  private static BootstrapBaseFileSplit split() throws IOException {
    return new BootstrapBaseFileSplit(
        new FileSplit(SKELETON, 0, 100, (String[]) null),
        new FileSplit(EXTERNAL, 0, 100, (String[]) null));
  }

  /**
   * {@code SELECT COUNT(*)} projects nothing, so both "only one file is needed" cases apply at once and the
   * order they are tested in decides the answer. It has to be the skeleton: it is inside the table root, and
   * bootstrap keeps a one-to-one row correspondence, so the count is the same either way.
   */
  @Test
  void testCountStarReadsSkeletonSoSplitPathStaysInsideTable() throws IOException {
    BootstrapBaseFileSplit split = split();

    Option<FileSplit> resolved = HoodieParquetInputFormat.resolveSingleFileSplit(split, false, false);

    assertSame(split, resolved.get(),
        "a query projecting no columns must read the skeleton, whose path is inside the table root");
    assertEquals(SKELETON, resolved.get().getPath());
  }

  /** Only meta columns projected: the external file is not needed. */
  @Test
  void testMetaColumnsOnlyReadsTheSkeleton() throws IOException {
    BootstrapBaseFileSplit split = split();

    Option<FileSplit> resolved = HoodieParquetInputFormat.resolveSingleFileSplit(split, true, false);

    assertEquals(SKELETON, resolved.get().getPath());
  }

  /** Only external columns projected: the data lives there, so the external file is required. */
  @Test
  void testDataColumnsOnlyReadsTheExternalFile() throws IOException {
    BootstrapBaseFileSplit split = split();

    Option<FileSplit> resolved = HoodieParquetInputFormat.resolveSingleFileSplit(split, false, true);

    assertEquals(EXTERNAL, resolved.get().getPath(),
        "data columns exist only in the external file, so it must still be read");
  }

  /** Both projected: neither file alone will do, and the caller stitches them. */
  @Test
  void testBothProjectedNeedsStitching() throws IOException {
    Option<FileSplit> resolved = HoodieParquetInputFormat.resolveSingleFileSplit(split(), true, true);

    assertFalse(resolved.isPresent(),
        "when both files are needed the caller must stitch them rather than pick one");
  }
}
