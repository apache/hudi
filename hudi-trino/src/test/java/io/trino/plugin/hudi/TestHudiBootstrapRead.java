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
package io.trino.plugin.hudi;

import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.memory.MemoryFileSystem;
import io.trino.metastore.HiveType;
import io.trino.parquet.writer.ParquetSchemaConverter;
import io.trino.parquet.writer.ParquetWriter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hudi.split.HudiSplitFactory;
import io.trino.plugin.hudi.split.HudiSplitWeightProvider;
import io.trino.spi.Page;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.testing.TestingConnectorSession;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.parquet.format.CompressionCodec;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHudiBootstrapRead
{
    private static final String INSTANT = "20260101000000000";

    @Test
    public void testReadsExternalBootstrapData()
            throws Exception
    {
        MemoryFileSystem fs = new MemoryFileSystem();
        String skeletonPath = "memory:///table/fileid_0-0-0_" + INSTANT + ".parquet";
        String sourcePath = "memory:///source/data.parquet";
        byte[] data = parquet("id", BIGINT, 42L);
        byte[] skeleton = parquet("_hoodie_record_key", VARCHAR, utf8Slice("key1"));
        fs.newOutputFile(Location.of(sourcePath)).createOrOverwrite(data);
        fs.newOutputFile(Location.of(skeletonPath)).createOrOverwrite(skeleton);
        var external = new BaseFile(file(sourcePath, data.length));
        var baseFile = new HoodieBaseFile(file(skeletonPath, skeleton.length), external);
        var slice = new FileSlice("", INSTANT, "fileid");
        slice.setBaseFile(baseFile);
        HudiSplit actual = factory().createSplits(List.of(), slice, INSTANT).getFirst();
        // Same source file the old getStoragePathInfo() branch selected for a bootstrapped base file.
        var externalSlice = new FileSlice("", INSTANT, "fileid");
        externalSlice.setBaseFile(new HoodieBaseFile(external.getPathInfo(), "fileid", INSTANT, null));
        HudiSplit control = factory().createSplits(List.of(), externalSlice, INSTANT).getFirst();
        Object expected = read(fs, control);
        Object result = read(fs, actual);
        assertThat(expected).isEqualTo(42L);
        assertThat(result).isEqualTo(expected);
    }

    private static Object read(MemoryFileSystem fs, HudiSplit split)
            throws Exception
    {
        var config = new ParquetReaderConfig();
        var session = TestingConnectorSession.builder().setPropertyMetadata(new HudiSessionProperties(new HudiConfig(), config).getSessionProperties()).build();
        var column = HiveColumnHandle.createBaseColumn("id", 0, HiveType.HIVE_LONG, BIGINT, HiveColumnHandle.ColumnType.REGULAR, Optional.empty());
        var provider = new HudiPageSourceProvider(identity -> fs, new FileFormatDataSourceStats(), config);
        try (var source = provider.createPageSource(null, session, split, serializedHandle(), Optional.empty(), List.of(column), DynamicFilter.EMPTY)) {
            while (!source.isFinished()) {
                var page = source.getNextSourcePage();
                if (page != null && page.getPositionCount() > 0) {
                    var block = page.getBlock(0);
                    return block.isNull(0) ? null : BIGINT.getLong(block, 0);
                }
            }
        }
        throw new AssertionError("No rows returned");
    }

    private static byte[] parquet(String name, Type type, Object value)
            throws Exception
    {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        var schema = new ParquetSchemaConverter(List.of(type), List.of(name), false, false);
        var builder = type.createBlockBuilder(null, 1);
        writeNativeValue(type, builder, value);
        try (var writer = new ParquetWriter(bytes, schema.getMessageType(), schema.getPrimitiveTypes(),
                ParquetWriterOptions.builder().build(), CompressionCodec.UNCOMPRESSED, "test",
                Optional.of(DateTimeZone.UTC), Optional.empty())) {
            writer.write(new Page(builder.build()));
        }
        return bytes.toByteArray();
    }

    private static HudiTableHandle serializedHandle()
    {
        return new HudiTableHandle("tests", "t", "memory:///table", HoodieTableType.COPY_ON_WRITE,
                List.of(), List.of(), TupleDomain.all(), TupleDomain.all(), OptionalLong.empty(), "", INSTANT);
    }

    private static StoragePathInfo file(String path, long size)
    {
        return new StoragePathInfo(new StoragePath(path), size, false, (short) 0, size, 1234);
    }

    private static HudiSplitFactory factory()
    {
        return new HudiSplitFactory(serializedHandle(), HudiSplitWeightProvider.uniformStandardWeightProvider(),
                DataSize.of(128, DataSize.Unit.MEGABYTE));
    }

}
