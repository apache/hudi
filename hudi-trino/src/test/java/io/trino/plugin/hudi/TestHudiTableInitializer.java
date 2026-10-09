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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.memory.MemoryFileSystem;
import io.trino.plugin.hudi.storage.HudiTrinoStorage;
import io.trino.plugin.hudi.storage.TrinoStorageConfiguration;
import io.trino.plugin.hudi.util.HudiSchemaConverter;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.keygen.KeyGenUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Map;

import static io.trino.plugin.hudi.HudiTableProperties.HIVE_STYLE_PARTITIONING_PROPERTY;
import static io.trino.plugin.hudi.HudiTableProperties.ORDERING_FIELDS_PROPERTY;
import static io.trino.plugin.hudi.HudiTableProperties.PARTITIONED_BY_PROPERTY;
import static io.trino.plugin.hudi.HudiTableProperties.PRIMARY_KEY_PROPERTY;
import static io.trino.plugin.hudi.HudiTableProperties.TABLE_TYPE_PROPERTY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers {@link HudiTableInitializer} in isolation from the metastore by inspecting the table
 * configuration written through an in-memory Trino file system. The storage extension-point
 * contract is covered separately by {@code TestHudiTrinoStorage}.
 */
final class TestHudiTableInitializer
{
    private static final String BASE_PATH = "memory:///warehouse/trips";

    @ParameterizedTest
    @EnumSource(HoodieTableType.class)
    void testInitializesReadableTableMetadata(HoodieTableType tableType)
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        initialize(fileSystem, tableType, ImmutableList.of("city"));

        HoodieTableConfig tableConfig = loadMetaClient(fileSystem).getTableConfig();
        assertThat(tableConfig.getTableType()).isEqualTo(tableType);
        assertThat(tableConfig.getTableName()).isEqualTo("trips");
        assertThat(tableConfig.getRecordKeyFields().get()).containsExactly("id");
        assertThat(tableConfig.getPartitionFields().get()).containsExactly("city");
        assertThat(tableConfig.getProps()).containsEntry(
                HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE.key(), "true");
    }

    @ParameterizedTest
    @CsvSource({
            "0, 0", "0, 1", "0, 2",
            "1, 0", "1, 1", "1, 2",
            "2, 0", "2, 1", "2, 2"})
    void testInferredKeyGeneratorTypeMatchesHudi(int keyCount, int partitionCount)
    {
        List<String> primaryKey = ImmutableList.of("id", "event_time").subList(0, keyCount);
        List<String> partitionedBy = ImmutableList.of("city", "region").subList(0, partitionCount);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("sales", "trips"),
                columns(),
                ImmutableMap.of(
                        PRIMARY_KEY_PROPERTY, primaryKey,
                        PARTITIONED_BY_PROPERTY, partitionedBy));
        TrinoFileSystem fileSystem = new MemoryFileSystem();

        HudiTableInitializer.initializeTable(fileSystem, BASE_PATH, tableMetadata, schema());

        assertThat(loadMetaClient(fileSystem).getTableConfig().getString(HoodieTableConfig.KEY_GENERATOR_TYPE))
                .isEqualTo(KeyGenUtils.inferKeyGeneratorType(
                        Option.ofNullable(primaryKey.isEmpty() ? null : String.join(",", primaryKey)),
                        String.join(",", partitionedBy)).name());
    }

    @Test
    void testHiveStylePartitioningCanBeDisabled()
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        HudiTableInitializer.initializeTable(
                fileSystem,
                BASE_PATH,
                tableMetadata(
                        HoodieTableType.COPY_ON_WRITE,
                        ImmutableList.of("city"),
                        ImmutableMap.of(),
                        ImmutableMap.of(HIVE_STYLE_PARTITIONING_PROPERTY, false)),
                schema());

        assertThat(loadMetaClient(fileSystem).getTableConfig().getProps()).containsEntry(
                HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE.key(), "false");
    }

    @Test
    void testTableVersionIsPinnedNotInherited()
    {
        // Keep the expected version literal so changing CREATED_TABLE_VERSION requires a deliberate test update.
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        initialize(fileSystem, HoodieTableType.COPY_ON_WRITE, ImmutableList.of());

        assertThat(loadMetaClient(fileSystem).getTableConfig().getTableVersion())
                .isEqualTo(HoodieTableVersion.TEN);
    }

    @Test
    void testCreateSchemaCarriesOnlyDataColumns()
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        initialize(fileSystem, HoodieTableType.COPY_ON_WRITE, ImmutableList.of("city"));

        String createSchema = loadMetaClient(fileSystem).getTableConfig()
                .getString(HoodieTableConfig.CREATE_SCHEMA);
        assertThat(createSchema)
                .doesNotContain("_hoodie_commit_time")
                .doesNotContain("_hoodie_record_key")
                .contains("\"name\":\"id\"")
                .contains("\"name\":\"city\"");
    }

    @Test
    void testOrderingFieldsAreWrittenUnderTheCurrentKey()
    {
        // hoodie.table.precombine.field is only a deprecated alternative of
        // hoodie.table.ordering.fields; a table created now must use the current key.
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        initialize(fileSystem, HoodieTableType.MERGE_ON_READ, ImmutableList.of());

        HoodieTableConfig tableConfig = loadMetaClient(fileSystem).getTableConfig();
        assertThat(tableConfig.getString(HoodieTableConfig.ORDERING_FIELDS)).isEqualTo("event_time");
    }

    @Test
    void testHoodiePassthroughReachesTableConfig()
    {
        // Only configs in HoodieTableConfig.PERSISTED_CONFIG_LIST survive TableBuilder#set; the
        // property validator rejects anything else rather than letting it vanish here.
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        HudiTableInitializer.initializeTable(
                fileSystem,
                BASE_PATH,
                tableMetadata(HoodieTableType.COPY_ON_WRITE, ImmutableList.of(), ImmutableMap.of(
                        "hoodie.keygen.timebased.timestamp.type", "DATE_STRING",
                        "hoodie.keygen.timebased.output.dateformat", "yyyy/MM/dd")),
                schema());

        HoodieTableConfig tableConfig = loadMetaClient(fileSystem).getTableConfig();
        assertThat(tableConfig.getString("hoodie.keygen.timebased.timestamp.type")).isEqualTo("DATE_STRING");
        assertThat(tableConfig.getString("hoodie.keygen.timebased.output.dateformat")).isEqualTo("yyyy/MM/dd");
    }

    private static void initialize(TrinoFileSystem fileSystem, HoodieTableType tableType, List<String> partitionedBy)
    {
        HudiTableInitializer.initializeTable(
                fileSystem, BASE_PATH, tableMetadata(tableType, partitionedBy, ImmutableMap.of()), schema());
    }

    private static ConnectorTableMetadata tableMetadata(
            HoodieTableType tableType,
            List<String> partitionedBy,
            Map<String, String> hoodieProperties)
    {
        return tableMetadata(tableType, partitionedBy, hoodieProperties, ImmutableMap.of());
    }

    private static ConnectorTableMetadata tableMetadata(
            HoodieTableType tableType,
            List<String> partitionedBy,
            Map<String, String> hoodieProperties,
            Map<String, Object> additionalProperties)
    {
        return new ConnectorTableMetadata(
                new SchemaTableName("sales", "trips"),
                columns(),
                ImmutableMap.<String, Object>builder()
                        .put(TABLE_TYPE_PROPERTY, tableType)
                        .put(PRIMARY_KEY_PROPERTY, ImmutableList.of("id"))
                        .put(ORDERING_FIELDS_PROPERTY, ImmutableList.of("event_time"))
                        .put(PARTITIONED_BY_PROPERTY, partitionedBy)
                        .put(HudiTableProperties.HOODIE_PROPERTIES_PROPERTY, hoodieProperties)
                        .putAll(additionalProperties)
                        .buildOrThrow());
    }

    private static List<ColumnMetadata> columns()
    {
        return ImmutableList.of(
                ColumnMetadata.builder().setName("id").setType(BIGINT).build(),
                ColumnMetadata.builder().setName("event_time").setType(createTimestampType(6)).build(),
                ColumnMetadata.builder().setName("city").setType(VARCHAR).build(),
                ColumnMetadata.builder().setName("region").setType(VARCHAR).build());
    }

    private static HoodieSchema schema()
    {
        return HudiSchemaConverter.toTableSchema(columns(), "trips");
    }

    private static HoodieTableMetaClient loadMetaClient(TrinoFileSystem fileSystem)
    {
        return HoodieTableMetaClient.builder()
                .setStorage(new HudiTrinoStorage(fileSystem, new TrinoStorageConfiguration()))
                .setBasePath(BASE_PATH)
                .build();
    }
}
