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
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.OptionalLong;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHudiSplitFactory
{
    private static final String COMMIT_TIME = "20250625153731546";
    private static final List<HivePartitionKey> PARTITION_KEYS = ImmutableList.of();

    @Test
    public void testCreateHudiSplitsWithSmallBaseFile()
    {
        // Test with 20MB target split size and 10MB base file
        // - should create 1 split
        testSplitCreation(
                DataSize.of(20, MEGABYTE),
                DataSize.of(10, MEGABYTE),
                Option.empty(),
                ImmutableList.of(
                        Pair.of(0L, DataSize.of(10, MEGABYTE))));
    }

    @Test
    public void testCreateHudiSplitsWithExactSplitDivide()
    {
        // Test with 20MB target and 60MB base file
        // - should create 3 splits
        testSplitCreation(
                DataSize.of(20, MEGABYTE),
                DataSize.of(60, MEGABYTE),
                Option.empty(),
                ImmutableList.of(
                        Pair.of(0L, DataSize.of(20, MEGABYTE)),
                        Pair.of(DataSize.of(20, MEGABYTE).toBytes(), DataSize.of(20, MEGABYTE)),
                        Pair.of(DataSize.of(40, MEGABYTE).toBytes(), DataSize.of(20, MEGABYTE))));
    }

    @Test
    public void testCreateHudiSplitsWithSlightlyOversizedFile()
    {
        // Test with 20MB target and 61MB base file
        // - should create 3 splits (61/20 = 3.05, 0.05 is within split slop of 0.1)
        testSplitCreation(
                DataSize.of(20, MEGABYTE),
                DataSize.of(61, MEGABYTE),
                Option.empty(),
                ImmutableList.of(
                        Pair.of(0L, DataSize.of(20, MEGABYTE)),
                        Pair.of(DataSize.of(20, MEGABYTE).toBytes(), DataSize.of(20, MEGABYTE)),
                        Pair.of(DataSize.of(40, MEGABYTE).toBytes(), DataSize.of(21, MEGABYTE))));
    }

    @Test
    public void testCreateHudiSplitsWithOversizedFileExceedingSlop()
    {
        // Test with 20MB target and 65MB base file
        // - should create 4 splits (65/20 = 3.25)
        testSplitCreation(
                DataSize.of(20, MEGABYTE),
                DataSize.of(65, MEGABYTE),
                Option.empty(),
                ImmutableList.of(
                        Pair.of(0L, DataSize.of(20, MEGABYTE)),
                        Pair.of(DataSize.of(20, MEGABYTE).toBytes(), DataSize.of(20, MEGABYTE)),
                        Pair.of(DataSize.of(40, MEGABYTE).toBytes(), DataSize.of(20, MEGABYTE)),
                        Pair.of(DataSize.of(60, MEGABYTE).toBytes(), DataSize.of(5, MEGABYTE))));
    }

    @Test
    public void testCreateHudiSplitsIgnoresBlockSize()
    {
        // Test with 2MB target and 8MB base file whose reported block size is 8MB
        // - the block size must be ignored, so 4 splits of the 2MB target size are expected
        //   (previously the 8MB block size beat the target and produced 1 split of 8MB)
        testSplitCreation(
                DataSize.of(2, MEGABYTE),
                DataSize.of(8, MEGABYTE),
                Option.empty(),
                ImmutableList.of(
                        Pair.of(0L, DataSize.of(2, MEGABYTE)),
                        Pair.of(DataSize.of(2, MEGABYTE).toBytes(), DataSize.of(2, MEGABYTE)),
                        Pair.of(DataSize.of(4, MEGABYTE).toBytes(), DataSize.of(2, MEGABYTE)),
                        Pair.of(DataSize.of(6, MEGABYTE).toBytes(), DataSize.of(2, MEGABYTE))));
    }

    @Test
    public void testCreateHudiSplitsWithFileSmallerThanDefaultTarget()
    {
        // Regression test for the split inflation reported in trinodb/trino#29842 (hudi#19231):
        // a ~120MB file with the default 128MB target must produce exactly 1 split
        testSplitCreation(
                DataSize.of(128, MEGABYTE),
                DataSize.of(120, MEGABYTE),
                Option.empty(),
                ImmutableList.of(
                        Pair.of(0L, DataSize.of(120, MEGABYTE))));
    }

    @Test
    public void testCreateHudiSplitsWithFileLargerThanDefaultTarget()
    {
        // Test with 128MB target and 500MB base file
        // - should be sliced at target boundaries into 3 x 128MB + 116MB remainder, even though the
        //   reported block size (500MB, the file length) would otherwise force a single split
        testSplitCreation(
                DataSize.of(128, MEGABYTE),
                DataSize.of(500, MEGABYTE),
                Option.empty(),
                ImmutableList.of(
                        Pair.of(0L, DataSize.of(128, MEGABYTE)),
                        Pair.of(DataSize.of(128, MEGABYTE).toBytes(), DataSize.of(128, MEGABYTE)),
                        Pair.of(DataSize.of(256, MEGABYTE).toBytes(), DataSize.of(128, MEGABYTE)),
                        Pair.of(DataSize.of(384, MEGABYTE).toBytes(), DataSize.of(116, MEGABYTE))));
    }

    @Test
    public void testCreateHudiSplitsWithZeroTargetSplitSize()
    {
        // A zero target split size must be rejected on construction, before any file slice is seen,
        // instead of looping forever once split generation reaches a non-empty base file
        assertThatThrownBy(() -> new HudiSplitFactory(
                createTableHandle(),
                new SizeBasedSplitWeightProvider(0.05, DataSize.of(128, MEGABYTE)),
                DataSize.ofBytes(0)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("targetSplitSize");
    }

    @Test
    public void testCreateHudiSplitsWithLogFile()
    {
        // Test with 20MB target and 65MB base file and 10MB log file
        // - should create 1 split regardless of size
        testSplitCreation(
                DataSize.of(20, MEGABYTE),
                DataSize.of(65, MEGABYTE),
                Option.of(DataSize.of(10, MEGABYTE)),
                ImmutableList.of(
                        Pair.of(0L, DataSize.of(65, MEGABYTE))));
    }

    @Test
    public void testCreateHudiSplitsWithZeroSizeFile()
    {
        // Test with zero-size file - should create 1 split with zero size
        testSplitCreation(
                DataSize.of(128, MEGABYTE),
                DataSize.of(0, MEGABYTE),
                Option.empty(),
                ImmutableList.of(Pair.of(0L, DataSize.of(0, MEGABYTE))));
    }

    private static void testSplitCreation(
            DataSize targetSplitSize,
            DataSize baseFileSize,
            Option<DataSize> logFileSize,
            List<Pair<Long, DataSize>> expectedSplitInfo)
    {
        HudiTableHandle tableHandle = createTableHandle();
        HudiSplitWeightProvider weightProvider = new SizeBasedSplitWeightProvider(0.05, DataSize.of(128, MEGABYTE));

        FileSlice fileSlice = createFileSlice(baseFileSize, logFileSize);

        List<HudiSplit> splits = new HudiSplitFactory(tableHandle, weightProvider, targetSplitSize)
                .createSplits(PARTITION_KEYS, fileSlice, COMMIT_TIME);

        assertThat(splits).hasSize(expectedSplitInfo.size());

        for (int i = 0; i < expectedSplitInfo.size(); i++) {
            HudiSplit split = splits.get(i);
            assertThat(split.getBaseFile()).isPresent();
            assertThat(split.getBaseFile().get().getFileSize()).isEqualTo(baseFileSize.toBytes());
            assertThat(split.getBaseFile().get().getStart())
                    .isEqualTo(expectedSplitInfo.get(i).getLeft());
            assertThat(split.getBaseFile().get().getLength())
                    .isEqualTo(expectedSplitInfo.get(i).getRight().toBytes());
            assertThat(split.getCommitTime()).isEqualTo(COMMIT_TIME);
            assertThat(split.getLogFiles().size()).isEqualTo(logFileSize.isPresent() ? 1 : 0);
            long totalSize = logFileSize.isPresent() ?
                    baseFileSize.toBytes() + logFileSize.get().toBytes() : expectedSplitInfo.get(i).getRight().toBytes();
            assertThat(split.getSplitWeight()).isEqualTo(weightProvider.calculateSplitWeight(totalSize));
        }
    }

    private static HudiTableHandle createTableHandle()
    {
        return new HudiTableHandle(
                "test_schema",
                "test_table",
                "/test/path",
                HoodieTableType.MERGE_ON_READ,
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
