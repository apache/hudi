/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hudi.util;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hudi.HudiSplit;
import io.trino.plugin.hudi.file.HudiFile;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import static io.trino.metastore.Partitions.makePartName;
import static io.trino.plugin.hive.HiveColumnHandle.isFileModifiedTimeColumnHandle;
import static io.trino.plugin.hive.HiveColumnHandle.isFileSizeColumnHandle;
import static io.trino.plugin.hive.HiveColumnHandle.isPartitionColumnHandle;
import static io.trino.plugin.hive.HiveColumnHandle.isPathColumnHandle;
import static io.trino.plugin.hive.util.HiveUtil.getPrefilledColumnValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;

/**
 * Per-split constant values for the output columns that are not stored in the data file: Hive-style
 * partition columns and Trino's hidden metadata columns ({@code $path}, {@code $file_size},
 * {@code $file_modified_time}, {@code $partition}). Value computation delegates to
 * {@link io.trino.plugin.hive.util.HiveUtil#getPrefilledColumnValue}, the same implementation the Hive
 * connector uses for these columns (including the {@code "\N"} hive-null partition-value convention).
 * Note {@code $file_modified_time} is packed with the JVM default zone, as in the Hive connector; the
 * replaced Hudi-specific code pinned UTC (same instant, different rendered zone).
 */
public class PrefilledColumnValues
{
    // Absent-marker for the memo, so a not-yet-resolved column is distinguishable from one resolved to null
    private static final Object UNRESOLVED = new Object();

    private final Map<String, HivePartitionKey> partitionKeysByName;
    private final String partitionName;
    private final String filePath;
    private final long fileSize;
    private final long fileModifiedTime;
    // Resolved native value per column name, populated lazily. One instance belongs to one split and a
    // split is read by a single driver thread, so a plain HashMap is enough; it also has to hold nulls,
    // which a ConcurrentHashMap could not.
    private final Map<String, Object> resolvedValues = new HashMap<>();

    public static PrefilledColumnValues create(HudiSplit hudiSplit)
    {
        return new PrefilledColumnValues(hudiSplit);
    }

    private PrefilledColumnValues(HudiSplit hudiSplit)
    {
        List<HivePartitionKey> partitionKeys = hudiSplit.getPartitionKeys();
        // ImmutableMap preserves insertion order, so $partition renders the keys in the split's
        // partition-column order.
        ImmutableMap.Builder<String, HivePartitionKey> byName = ImmutableMap.builder();
        partitionKeys.forEach(partitionKey -> byName.put(partitionKey.name(), partitionKey));
        this.partitionKeysByName = byName.buildOrThrow();
        this.partitionName = makePartName(
                partitionKeys.stream().map(HivePartitionKey::name).toList(),
                partitionKeys.stream().map(HivePartitionKey::value).toList());
        // Parquet files will be prioritised over log files
        HudiFile hudiFile = hudiSplit.getBaseFile().isPresent()
                ? hudiSplit.getBaseFile().get()
                : hudiSplit.getLogFiles().getFirst();
        this.filePath = hudiFile.getPath();
        this.fileSize = hudiFile.getFileSize();
        this.fileModifiedTime = hudiFile.getModificationTime();
    }

    /**
     * Returns whether this split can provide a value for the column, i.e. it is a partition column of
     * the split or a hidden metadata column Hudi populates.
     */
    public boolean isPrefilled(HiveColumnHandle columnHandle)
    {
        return partitionKeysByName.containsKey(columnHandle.getName())
                || isPathColumnHandle(columnHandle)
                || isFileSizeColumnHandle(columnHandle)
                || isFileModifiedTimeColumnHandle(columnHandle)
                || isPartitionColumnHandle(columnHandle);
    }

    /**
     * Appends the column's value for this split to the builder; appends null for a column this split
     * cannot provide.
     */
    public void appendTo(HiveColumnHandle columnHandle, BlockBuilder blockBuilder)
    {
        writeNativeValue(columnHandle.getType(), blockBuilder, nativeValueOf(columnHandle));
    }

    /**
     * Builds a run-length-encoded {@link Block} repeating the column's constant value for
     * {@code positionCount} positions (null-filled for a column this split cannot provide).
     */
    public Block toRleBlock(HiveColumnHandle columnHandle, int positionCount)
    {
        return RunLengthEncodedBlock.create(columnHandle.getType(), nativeValueOf(columnHandle), positionCount);
    }

    private Object nativeValueOf(HiveColumnHandle columnHandle)
    {
        // Every input to computeNativeValue() is a constant of the split, but appendTo is called once per
        // prefilled column per record, and computing re-parses the partition string each time
        // ($file_modified_time even formats a timestamp and parses it straight back). Memoize per column so
        // each one is resolved once per split. Keyed on the name rather than the handle because
        // HiveColumnHandle.hashCode hashes seven fields through a varargs array, whereas a String caches
        // its hash. A sentinel rather than a null check, because null is a legitimate resolved value -- both
        // for the hive-null convention and for the lenient fallback below -- and getOrDefault keeps the hit
        // path, the one taken per record, to a single hash lookup.
        String name = columnHandle.getName();
        Object value = resolvedValues.getOrDefault(name, UNRESOLVED);
        if (value == UNRESOLVED) {
            value = computeNativeValue(columnHandle);
            resolvedValues.put(name, value);
        }
        return value;
    }

    private Object computeNativeValue(HiveColumnHandle columnHandle)
    {
        if (!isPrefilled(columnHandle)) {
            // Lenient null fill, e.g. for a hidden column Trino defines but Hudi does not populate.
            return null;
        }
        HivePartitionKey partitionKey = partitionKeysByName.get(columnHandle.getName());
        return getPrefilledColumnValue(
                columnHandle,
                partitionKey,
                filePath,
                // Hudi tables are never hive-bucketed, so no $bucket value can be requested here
                OptionalInt.empty(),
                fileSize,
                fileModifiedTime,
                partitionName)
                .getValue();
    }
}
