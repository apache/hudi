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
import org.apache.hudi.storage.StoragePath;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.trino.plugin.hudi.HudiTableInitializer.CREATED_TABLE_VERSION;
import static io.trino.plugin.hudi.HudiTableProperties.ORDERING_FIELDS_PROPERTY;
import static io.trino.plugin.hudi.HudiTableProperties.PARTITIONED_BY_PROPERTY;
import static io.trino.plugin.hudi.HudiTableProperties.PRIMARY_KEY_PROPERTY;
import static io.trino.plugin.hudi.HudiTableProperties.TABLE_TYPE_PROPERTY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers table initialization in isolation from the metastore, and in particular that it works at
 * all through {@link HudiTrinoStorage}: the connector has to use the {@code HoodieStorage} overload
 * of {@code initTable} because the configuration-based one resolves storage reflectively via a
 * {@code (StoragePath, StorageConfiguration)} constructor that a session-scoped
 * {@code TrinoFileSystem} cannot supply.
 */
final class TestHudiTableInitializer
{
    private static final String BASE_PATH = "memory:///warehouse/trips";

    @Test
    void testInitializesReadableTableMetadata()
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        initialize(fileSystem, HoodieTableType.COPY_ON_WRITE, ImmutableList.of("city"));

        HoodieTableConfig tableConfig = loadMetaClient(fileSystem).getTableConfig();
        assertThat(tableConfig.getTableType()).isEqualTo(HoodieTableType.COPY_ON_WRITE);
        assertThat(tableConfig.getTableName()).isEqualTo("trips");
        assertThat(tableConfig.getRecordKeyFields().get()).containsExactly("id");
        assertThat(tableConfig.getPartitionFields().get()).containsExactly("city");
    }

    @Test
    void testMergeOnReadIsInitializable()
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        initialize(fileSystem, HoodieTableType.MERGE_ON_READ, ImmutableList.of());

        assertThat(loadMetaClient(fileSystem).getTableConfig().getTableType())
                .isEqualTo(HoodieTableType.MERGE_ON_READ);
    }

    @Test
    void testTableVersionIsPinnedNotInherited()
    {
        // Guards the pin: if HoodieTableVersion.current() moves ahead of CREATED_TABLE_VERSION, that
        // is a deliberate decision and this assertion is where it has to be made.
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        initialize(fileSystem, HoodieTableType.COPY_ON_WRITE, ImmutableList.of());

        assertThat(loadMetaClient(fileSystem).getTableConfig().getTableVersion())
                .isEqualTo(CREATED_TABLE_VERSION);
    }

    @Test
    void testCreateSchemaCarriesMetaFieldsAndDataColumns()
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        initialize(fileSystem, HoodieTableType.COPY_ON_WRITE, ImmutableList.of("city"));

        String createSchema = loadMetaClient(fileSystem).getTableConfig()
                .getString(HoodieTableConfig.CREATE_SCHEMA);
        assertThat(createSchema)
                .contains("_hoodie_commit_time")
                .contains("_hoodie_record_key")
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

    @Test
    void testInitGoesThroughHudisPluggableStorageExtensionPoint()
    {
        // Proves which code path table init actually takes. TrinoStorageConfiguration names
        // HudiTrinoStorage as HOODIE_STORAGE_CLASS, so hudi-common builds storage reflectively via
        // HoodieStorageUtils#getStorage. Handing it a configuration with no file system must fail in
        // HudiTrinoStorage's extension-point constructor -- if init were reaching storage some other
        // way, this would not throw.
        assertThatThrownBy(() -> HoodieTableMetaClient.newTableBuilder()
                .setTableType(HoodieTableType.COPY_ON_WRITE)
                .setTableName("trips")
                .setTableCreateSchema(schema().toAvroSchema().toString())
                .initTable(new TrinoStorageConfiguration(), new StoragePath(BASE_PATH)))
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .rootCause()
                .hasMessageContaining("carries no file system");
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
        return new ConnectorTableMetadata(
                new SchemaTableName("sales", "trips"),
                columns(),
                ImmutableMap.<String, Object>builder()
                        .put(TABLE_TYPE_PROPERTY, tableType)
                        .put(PRIMARY_KEY_PROPERTY, ImmutableList.of("id"))
                        .put(ORDERING_FIELDS_PROPERTY, ImmutableList.of("event_time"))
                        .put(PARTITIONED_BY_PROPERTY, partitionedBy)
                        .put(HudiTableProperties.HOODIE_PROPERTIES_PROPERTY, hoodieProperties)
                        .buildOrThrow());
    }

    private static List<ColumnMetadata> columns()
    {
        return ImmutableList.of(
                ColumnMetadata.builder().setName("id").setType(BIGINT).build(),
                ColumnMetadata.builder().setName("event_time").setType(createTimestampType(6)).build(),
                ColumnMetadata.builder().setName("city").setType(VARCHAR).build());
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
