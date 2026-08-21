/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.keygen;

import org.apache.spark.unsafe.types.UTF8String;

import java.util.List;
import java.util.function.Supplier;

import static org.apache.hudi.common.util.ValidationUtils.checkState;
import static org.apache.hudi.keygen.KeyGenUtils.DEFAULT_PARTITION_PATH_SEPARATOR;

/**
 * Partition path formatter allows to efficiently combine partition paths into
 * generic Java {@link String} (as well as engine-specific containers like
 * {@code UTF8String} for ex), while being flexible in terms of
 *
 * <ul>
 *   <li>Allowing to configure how to handle empty values</li>
 *   <li>Allowing to encode individual values</li>
 *   <li>Supporting Hive-style partitioning ({@code column=value})</li>
 * </ul>
 *
 * @param <S> string type
 */
public abstract class PartitionPathFormatterBase<S> {

  private final Supplier<StringBuilder<S>> stringBuilderFactory;

  private final boolean useHiveStylePartitioning;
  private final boolean useEncoding;
  private final boolean slashSeparatedDatePartitioning;

  PartitionPathFormatterBase(Supplier<StringBuilder<S>> stringBuilderFactory,
                             boolean useHiveStylePartitioning,
                             boolean useEncoding,
                             boolean slashSeparatedDatePartitioning) {
    this.stringBuilderFactory = stringBuilderFactory;

    this.useHiveStylePartitioning = useHiveStylePartitioning;
    this.useEncoding = useEncoding;
    this.slashSeparatedDatePartitioning = slashSeparatedDatePartitioning;
  }

  public final S combine(List<String> partitionPathFields, Object... partitionPathParts) {
    checkState(partitionPathParts.length == partitionPathFields.size());
    // Avoid creating [[StringBuilder]] in case there's just one partition-path part,
    // and Hive-style of partitioning is not required
    if (!useHiveStylePartitioning && partitionPathParts.length == 1) {
      S partitionPathPart = tryEncode(handleEmpty(toString(partitionPathParts[0])));
      // NOTE: See [[replaceDashesWithSlashes]] on how this lines up with the Avro write path, and
      //       [[hasPathBreakingDash]] on which dashes suppress the substitution
      return slashSeparatedDatePartitioning && !hasPathBreakingDash(partitionPathPart)
          ? replaceDashesWithSlashes(partitionPathPart)
          : partitionPathPart;
    }

    StringBuilder<S> sb = stringBuilderFactory.get();
    for (int i = 0; i < partitionPathParts.length; ++i) {
      S partitionPathPartStr = tryEncode(handleEmpty(toString(partitionPathParts[i])));

      if (useHiveStylePartitioning) {
        sb.appendJava(partitionPathFields.get(i))
            .appendJava("=")
            .append(partitionPathPartStr);
      } else if (slashSeparatedDatePartitioning && !hasPathBreakingDash(partitionPathPartStr)) {
        // NOTE: Every part is substituted here, preserving the behaviour this branch had before the
        //       [[ClassCastException]] fix. [[CustomKeyGenerator]] builds one single-field sub-keygen
        //       per field, so its write path slashes each field individually, and
        //       [[SparkHoodieTableFileIndex#composeRelativePartitionPath]] has to land on the same
        //       directory when it composes the prefix in one [[combine]] call over all N columns.
        //       Whether a multi-field table should be slash-separated at all is HUDI issue #19666
        sb.append(replaceDashesWithSlashes(partitionPathPartStr));
      } else {
        sb.append(partitionPathPartStr);
      }

      if (i < partitionPathParts.length - 1) {
        sb.appendJava(DEFAULT_PARTITION_PATH_SEPARATOR);
      }
    }

    return sb.build();
  }

  private S tryEncode(S partitionPathPart) {
    return useEncoding ? encode(partitionPathPart) : partitionPathPart;
  }

  protected abstract S toString(Object o);

  protected abstract S encode(S partitionPathPart);

  protected abstract S handleEmpty(S partitionPathPart);

  /**
   * Turns a {@code yyyy-MM-dd} formatted date value into the {@code yyyy/MM/dd} directory structure
   * requested by {@code hoodie.datasource.write.slash.separated.date.partitioning}.
   *
   * <p>NOTE: This has to be implemented by every sub-class, since the substitution has to be
   * performed on the concrete string representation {@code S} the formatter operates on.
   *
   * <p>NOTE: For {@code SimpleKeyGenerator}/{@code ComplexKeyGenerator} the single-part branch of
   * {@link #combine} routes only a table partitioned by a single (date) column here, mirroring
   * {@code KeyGenUtils#getPartitionPath} (single field) and {@code KeyGenUtils#getRecordPartitionPath}
   * (which guards on a single field as well) driving the Avro write-path: both write-paths have to
   * derive the very same partition path for a record.
   *
   * <p>NOTE: The multi-part branch substitutes every part, which {@code CustomKeyGenerator} depends
   * on: it builds one single-field sub-key-generator per partition field, so its own write path
   * slashes each field individually, and {@code SparkHoodieTableFileIndex#composeRelativePartitionPath}
   * -- which calls {@link #combine} once over all N columns to compose a listing prefix -- has to
   * name the very same directory, or the prefix misses and the query silently returns no rows. For
   * {@code ComplexKeyGenerator} this leaves the row-writer path slashing where the Avro path does
   * not, and neither layout reads back: the extra fragments leave
   * {@code HoodieSparkUtils#doParsePartitionColumnValues} unable to line the path up with the
   * partition columns. Both are tracked in HUDI issue #19666.
   */
  protected abstract S replaceDashesWithSlashes(S partitionPathPart);

  /**
   * Whether substituting the dashes in {@code partitionPathPart} would yield a leading, trailing or
   * doubled path separator, in which case {@link #replaceDashesWithSlashes(Object)} must not be
   * applied to it. Kept in step with {@code KeyGenUtils#hasPathBreakingDash}, which guards the Avro
   * write path.
   *
   * <p>NOTE: This has to be implemented by every sub-class, since the check has to be performed on
   * the concrete string representation {@code S} the formatter operates on.
   *
   * <p>NOTE: None of the three shapes survives the round trip back from storage. A leading dash
   * makes the path absolute, and that is resolved inconsistently --
   * {@code FSUtils#constructAbsolutePath(String, String)} chops the leading "/" while the
   * {@code StoragePath} overload used by {@code AbstractTableFileSystemView} lets it URI-resolve away
   * the table base path ({@code "-5"} lands the writer in {@code <base>/5} but the file-system view
   * in {@code /5}). A trailing dash gives {@code "5/"}, which {@code StoragePath} normalizes back to
   * {@code "5"}, and a doubled dash gives {@code "a//b"}, which {@code URI.normalize()} collapses to
   * {@code "a/b"} -- in both cases the writer records a partition string longer than the directory
   * it resolves to, so {@code FSUtils#getFileName} slices the file name at the wrong offset. None of
   * these values is a date to begin with, so nothing is lost by not slashing them.
   */
  protected abstract boolean hasPathBreakingDash(S partitionPathPart);

  /**
   * This is a generic interface closing the gap and unifying the {@link java.lang.StringBuilder} with
   * {@link org.apache.hudi.unsafe.UTF8StringBuilder} implementations, allowing us to avoid code-duplication by performing
   * most of the key-generation in a generic and unified way
   *
   * @param <S> target string type this builder is producing (could either be native {@link String}
   *           or alternatively {@link UTF8String}
   */
  interface StringBuilder<S> {
    default StringBuilder<S> append(S s) {
      return appendJava(s.toString());
    }

    StringBuilder<S> appendJava(String s);

    S build();
  }
}
