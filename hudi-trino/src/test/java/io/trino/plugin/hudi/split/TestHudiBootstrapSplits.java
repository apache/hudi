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
package io.trino.plugin.hudi.split;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hudi.HudiSplit;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.OptionalLong;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHudiBootstrapSplits
{
    private static final String COMMIT_TIME = "20250625153731546";
    private static final List<HivePartitionKey> PARTITION_KEYS = ImmutableList.of();

    @Test
    public void testBootstrapSplitsUseExternalFilePathAndSize()
    {
        FileSlice slice = createFileSlice(DataSize.of(1, MEGABYTE), Option.empty());
        BaseFile external = new BaseFile(new StoragePathInfo(
                new StoragePath("/source/part-00000.parquet"), DataSize.of(65, MEGABYTE).toBytes(),
                false, (short) 0, 0, 12345));
        slice.getBaseFile().get().setBootstrapBaseFile(external);

        for (HoodieTableType type : HoodieTableType.values()) {
            List<HudiSplit> splits = new HudiSplitFactory(createTableHandle(type),
                    HudiSplitWeightProvider.uniformStandardWeightProvider(), DataSize.of(20, MEGABYTE))
                    .createSplits(PARTITION_KEYS, slice, COMMIT_TIME);
            assertThat(splits).hasSize(4);
            long position = 0;
            for (HudiSplit split : splits) {
                var file = split.getBaseFile().orElseThrow();
                assertThat(file.getPath()).isEqualTo(external.getPath());
                assertThat(file.getFileSize()).isEqualTo(external.getFileSize());
                assertThat(file.getModificationTime()).isEqualTo(12345);
                assertThat(file.getStart()).isEqualTo(position);
                position += file.getLength();
            }
            assertThat(position).isEqualTo(external.getFileSize());
        }
    }

    @Test
    public void testBootstrapWithLogsFailsInsteadOfReadingSkeletonData()
    {
        FileSlice slice = createFileSlice(DataSize.of(1, MEGABYTE), Option.of(DataSize.of(1, MEGABYTE)));
        slice.getBaseFile().get().setBootstrapBaseFile(new BaseFile(new StoragePathInfo(
                new StoragePath("/source/part-00000.parquet"), 100, false, (short) 0, 100, 1)));
        assertThatThrownBy(() -> new HudiSplitFactory(createTableHandle(),
                HudiSplitWeightProvider.uniformStandardWeightProvider(), DataSize.of(128, MEGABYTE))
                .createSplits(PARTITION_KEYS, slice, COMMIT_TIME))
                .isInstanceOf(io.trino.spi.TrinoException.class)
                .hasMessageContaining("metadata-only bootstrap");
        assertThat(new HudiSplitFactory(createTableHandle(HoodieTableType.COPY_ON_WRITE),
                HudiSplitWeightProvider.uniformStandardWeightProvider(), DataSize.of(128, MEGABYTE))
                .createSplits(PARTITION_KEYS, slice, COMMIT_TIME))
                .singleElement().satisfies(split -> assertThat(split.getBaseFile().orElseThrow().getPath())
                        .isEqualTo("/source/part-00000.parquet"));
    }

    private static HudiTableHandle createTableHandle()
    {
        return createTableHandle(HoodieTableType.MERGE_ON_READ);
    }

    private static HudiTableHandle createTableHandle(HoodieTableType tableType)
    {
        return new HudiTableHandle(
                "test_schema",
                "test_table",
                "/test/path",
                tableType,
                ImmutableList.of(),
                ImmutableList.of(),
                TupleDomain.all(),
                TupleDomain.all(),
                OptionalLong.empty(),
                "",
                "101");
    }

    private static FileSlice createFileSlice(DataSize baseFileSize, Option<DataSize> logFileSize)
    {
        String fileId = "5a4f6a70-0306-40a8-952b-045b0d8ff0d4-0";
        HoodieFileGroupId fileGroupId = new HoodieFileGroupId("partition", fileId);
        // Block size mirrors the file length, which is what HudiTrinoStorage now reports. Split
        // generation must ignore it, so every multi-split expectation below would collapse to a
        // single whole-file split if the block size were allowed back into the sizing decision.
        String baseFilePath = "/test/path/" + fileGroupId + "_4-19-0_" + COMMIT_TIME + ".parquet";
        String logFilePath = "/test/path/." + fileId + "_2025062515374131546.log.1_0-53-80";
        long logFileSizeInBytes = logFileSize.isPresent() ? logFileSize.get().toBytes() : 0L;
        StoragePathInfo baseFileInfo = new StoragePathInfo(
                new StoragePath(baseFilePath), baseFileSize.toBytes(), false, (short) 0, baseFileSize.toBytes(), System.currentTimeMillis());
        StoragePathInfo logFileInfo = new StoragePathInfo(
                new StoragePath(logFilePath), logFileSizeInBytes,
                false, (short) 0, logFileSizeInBytes, System.currentTimeMillis());
        HoodieBaseFile baseFile = new HoodieBaseFile(baseFileInfo);
        return new FileSlice(fileGroupId, COMMIT_TIME, baseFile,
                logFileSize.isPresent() ? ImmutableList.of(new HoodieLogFile(logFileInfo)) : ImmutableList.of());
    }
}
