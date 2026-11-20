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

import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.config.CloudSourceConfig;
import org.apache.hudi.utilities.sources.blob.BlobTableConfig;
import org.apache.hudi.utilities.sources.helpers.CloudObjectMetadata;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestUnstructuredObjectMaterializer {

  private static final long MIB = 1024L * 1024L;
  /** The inline threshold used to exercise inclusive sizing and copy partition costs. */
  private static final long INLINE_MAX = 64 * 1024;

  private static UnstructuredObjectMaterializer materializer(TypedProperties props, int defaultParallelism) {
    JavaSparkContext jsc = mock(JavaSparkContext.class);
    when(jsc.defaultParallelism()).thenReturn(defaultParallelism);
    return new UnstructuredObjectMaterializer(props, jsc);
  }

  private static UnstructuredObjectMaterializer materializer() {
    return materializer(new TypedProperties(), 4);
  }

  private static UnstructuredObjectMaterializer inlineUpTo64KiB(int defaultParallelism) {
    TypedProperties props = new TypedProperties();
    props.setProperty(BlobTableConfig.INLINE_MAX_BYTES.key(), String.valueOf(INLINE_MAX));
    return materializer(props, defaultParallelism);
  }

  @Test
  public void partitionCountSizesInlineBytesLikeTheColumnarMaterializer() {
    // 10 inline objects of 64 KiB (the inline maximum, inclusive) against 256 KiB per partition: ceil(10 x 1.1 / 4) = 3
    List<CloudObjectMetadata> objects = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      objects.add(new CloudObjectMetadata("s3://b/" + i + ".pdf", INLINE_MAX));
    }
    assertEquals(3, inlineUpTo64KiB(4).partitionCount(objects, 4 * INLINE_MAX, 0));
    assertEquals(7, inlineUpTo64KiB(4).partitionCount(objects, 4 * INLINE_MAX, 7), "the profile minimum is a lower bound");
    assertEquals(1, inlineUpTo64KiB(4).partitionCount(Collections.emptyList(), 4 * INLINE_MAX, 0), "never below one");
  }

  @Test
  public void partitionCountGivesEachOutOfLineCopyATaskUpToTheSlots() {
    // out-of-line bytes never reach the rows, so 3 large objects of 1 GiB count as 3 copies, not 3 GiB of row data
    List<CloudObjectMetadata> three = Arrays.asList(new CloudObjectMetadata("s3://b/a.mp4", 1024 * MIB),
        new CloudObjectMetadata("s3://b/b.mp4", 1024 * MIB), new CloudObjectMetadata("s3://b/c.mp4", 1024 * MIB));
    assertEquals(3, inlineUpTo64KiB(8).partitionCount(three, 128 * MIB, 0));
    assertEquals(2, inlineUpTo64KiB(2).partitionCount(three, 128 * MIB, 0), "capped at the slots");
    // the inline term wins when it asks for more: 400 x 64 KiB x 1.1 / 8 MiB = 3.4, so 4
    List<CloudObjectMetadata> mixed = new ArrayList<>(three);
    for (int i = 0; i < 400; i++) {
      mixed.add(new CloudObjectMetadata("s3://b/" + i + ".png", INLINE_MAX));
    }
    assertEquals(4, inlineUpTo64KiB(2).partitionCount(mixed, 8 * MIB, 0));
  }

  @Test
  public void partitionCountRefusesABytesPerPartitionThatIsNotPositive() {
    // zero would divide the inline bytes into Integer.MAX_VALUE partitions; a negative target would size nothing
    List<CloudObjectMetadata> objects = Collections.singletonList(new CloudObjectMetadata("s3://b/a.pdf", INLINE_MAX));
    for (long bytesPerPartition : new long[] {0, -1}) {
      IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
          () -> inlineUpTo64KiB(4).partitionCount(objects, bytesPerPartition, 0));
      assertTrue(e.getMessage().contains(CloudSourceConfig.SOURCE_MAX_BYTES_PER_PARTITION.key()), e.getMessage());
      assertTrue(e.getMessage().contains(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.key()), e.getMessage());
      assertTrue(e.getMessage().contains(String.valueOf(bytesPerPartition)), e.getMessage());
    }
  }

  private static CloudObjectMetadata entry(String path, long size, long notificationTime, String orderingToken) {
    return new CloudObjectMetadata(path, size, orderingToken);
  }

  /**
   * One entry per path, the latest by ordering token: the greatest token wins over any notification time or size, and an
   * entry with a token wins over one without. Paths keep their first-seen order, and where a token decides, the choice does
   * not depend on the order the entries arrive in.
   */
  @Test
  public void onePerPathKeepsTheEntryWithTheGreatestOrderingToken() {
    CloudObjectMetadata tokenOld = entry("s3://b/token.pdf", 900, 3000, "0001");
    CloudObjectMetadata tokenNew = entry("s3://b/token.pdf", 10, 1000, "0002");
    CloudObjectMetadata untokened = entry("s3://b/mixed.pdf", 900, 9000, null);
    CloudObjectMetadata tokened = entry("s3://b/mixed.pdf", 10, 1000, "0001");
    CloudObjectMetadata single = entry("s3://b/single.pdf", 5, 1000, null);
    List<CloudObjectMetadata> input = Arrays.asList(tokenOld, untokened, single, tokenNew, tokened);

    assertEquals(Arrays.asList(tokenNew, tokened, single), UnstructuredObjectMaterializer.onePerPath(input));
    List<CloudObjectMetadata> reversed = new ArrayList<>(input);
    Collections.reverse(reversed);
    assertEquals(new HashSet<>(Arrays.asList(tokenNew, tokened, single)),
        new HashSet<>(UnstructuredObjectMaterializer.onePerPath(reversed)), "the same entries whatever the arrival order");
  }

  /** Without a token to decide (no sequencer, or an equal one), the first entry seen is kept, whatever its time or size. */
  @Test
  public void onePerPathKeepsTheFirstEntryWhenNoTokenDecides() {
    CloudObjectMetadata first = entry("s3://b/a.pdf", 10, 1000, null);
    CloudObjectMetadata second = entry("s3://b/a.pdf", 20, 2000, null);
    assertEquals(Collections.singletonList(first), UnstructuredObjectMaterializer.onePerPath(Arrays.asList(first, second)));
    assertEquals(Collections.singletonList(second), UnstructuredObjectMaterializer.onePerPath(Arrays.asList(second, first)));
    CloudObjectMetadata equalTokenFirst = entry("s3://b/b.pdf", 10, 1000, "0001");
    CloudObjectMetadata equalTokenSecond = entry("s3://b/b.pdf", 20, 2000, "0001");
    assertEquals(Collections.singletonList(equalTokenFirst),
        UnstructuredObjectMaterializer.onePerPath(Arrays.asList(equalTokenFirst, equalTokenSecond)));
  }

  @Test
  public void onePerPathKeepsDistinctPathsAndAnEmptySelection() {
    List<CloudObjectMetadata> distinct = Arrays.asList(entry("s3://b/a.pdf", 1, 0, null), entry("s3://b/A.pdf", 1, 0, null),
        entry("s3://other/a.pdf", 1, 0, null));
    assertEquals(distinct, UnstructuredObjectMaterializer.onePerPath(distinct), "paths differing by case or bucket are distinct");
    assertTrue(UnstructuredObjectMaterializer.onePerPath(Collections.emptyList()).isEmpty());
  }

  /**
   * The size-sorted list is dealt round-robin over the partitions, so the largest objects land in different tasks; a
   * contiguous slicing would put the four large objects into the first two partitions.
   */
  @Test
  public void theSizeSortedListIsDealtRoundRobinOverThePartitions() {
    List<CloudObjectMetadata> objects = new ArrayList<>();
    // sorted by size descending, as materialize hands it over
    for (long size : new long[] {100, 90, 80, 70, 1, 1, 1, 1}) {
      objects.add(new CloudObjectMetadata("s3://b/" + objects.size() + ".pdf", size));
    }
    List<List<Long>> partitions = UnstructuredObjectMaterializer.deal(objects, 4).stream()
        .map(slice -> slice.stream().map(CloudObjectMetadata::getSize).collect(Collectors.toList()))
        .collect(Collectors.toList());
    assertEquals(Arrays.asList(Arrays.asList(100L, 1L), Arrays.asList(90L, 1L), Arrays.asList(80L, 1L), Arrays.asList(70L, 1L)),
        partitions);
  }

  @Test
  public void defaultPredicateUsesDottedExtensionsWithoutEventColumns() {
    String predicate = materializer().objectKeyPredicate("s3.object.key", new TypedProperties());
    assertEquals(" and (" + Arrays.stream(BlobTableConfig.FILE_EXTENSIONS.defaultValue().split(","))
        .map(extension -> "s3.object.key like '%." + extension + "'").collect(Collectors.joining(" or ")) + ")", predicate);
  }

  @Test
  public void emptyIntersectionIsFalseAndNarrowingUsesOneDottedSuffix() {
    TypedProperties props = new TypedProperties();
    props.setProperty(BlobTableConfig.FILE_EXTENSIONS.key(), "pdf");
    assertEquals(" and (s3.object.key like '%.pdf')", materializer(props, 4).objectKeyPredicate("s3.object.key", props));
    props.setProperty(CloudSourceConfig.CLOUD_DATAFILE_EXTENSION.key(), "png");
    assertEquals(" and false", materializer(props, 4).objectKeyPredicate("s3.object.key", props));
  }
}
