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

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.hudi.storage.TrinoStorageConfiguration;
import io.trino.plugin.hudi.testing.HudiTablesInitializer;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.storage.StoragePath;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;

final class TestHudiDdl
        extends AbstractTestQueryFramework
{
    private final UnregisteredTableInitializer initializer = new UnregisteredTableInitializer();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HudiQueryRunner.builder()
                .addConnectorProperty("hudi.register-table-procedure.enabled", "true")
                .setDataLoader(initializer)
                .build();
    }

    @Test
    void testRegisterAndUnregisterExistingTableWithoutChangingStorage()
    {
        String tableName = initializer.getTableName();
        String tableLocation = initializer.getTableLocation().toString();

        assertUpdate("CALL hudi.system.register_table(" +
                "schema_name => 'tests', " +
                "table_name => '" + tableName + "', " +
                "table_location => '" + tableLocation + "')");
        assertQueryFails(
                "CALL hudi.system.register_table('tests', '" + tableName + "', '" + tableLocation + "')",
                ".*Table already exists: tests\\." + tableName + ".*");

        assertQuery("SELECT count(*) FROM " + tableName, "VALUES CAST(0 AS BIGINT)");
        assertThat(initializer.getMetastore().getTable("tests", tableName)).get()
                .satisfies(table -> {
                    assertThat(table.getStorage().getLocation()).isEqualTo(tableLocation);
                    assertThat(table.getPartitionColumns()).extracting(io.trino.metastore.Column::getName)
                            .containsExactly("city");
                    assertThat(table.getParameters()).containsEntry("EXTERNAL", "TRUE");
                });
        assertThat(initializer.loadMetaClient().getTableConfig().getTableVersion())
                .isEqualTo(HoodieTableVersion.EIGHT);

        assertUpdate("CALL hudi.system.unregister_table(" +
                "schema_name => 'tests', " +
                "table_name => '" + tableName + "')");

        assertThat(initializer.getMetastore().getTable("tests", tableName)).isEmpty();
        assertThat(HudiUtil.hudiMetadataExists(initializer.getFileSystem(), initializer.getTableLocation())).isTrue();
        assertThat(initializer.loadMetaClient().getTableConfig().getTableVersion())
                .isEqualTo(HoodieTableVersion.EIGHT);
        assertQueryFails(
                "CALL hudi.system.unregister_table('tests', '" + tableName + "')",
                ".*Table 'tests\\." + tableName + "' not found.*");

        assertUpdate("CALL hudi.system.register_table('tests', '" + tableName + "', '" + tableLocation + "')");
        assertUpdate("DROP TABLE " + tableName);

        assertThat(initializer.getMetastore().getTable("tests", tableName)).isEmpty();
        assertThat(HudiUtil.hudiMetadataExists(initializer.getFileSystem(), initializer.getTableLocation())).isTrue();
    }

    @Test
    void testRegisterRejectsNonParquetTable()
            throws Exception
    {
        String tableName = "unregistered_orc_table";
        Location tableLocation = initializer.getExternalLocation().appendPath(tableName);
        initializer.initializeTable(tableName, tableLocation, "tests", HoodieFileFormat.ORC);

        assertQueryFails(
                "CALL hudi.system.register_table('tests', '" + tableName + "', '" + tableLocation + "')",
                ".*Cannot register Hudi table tests\\." + tableName + " with base file format ORC; only PARQUET is supported.*");
        assertThat(initializer.getMetastore().getTable("tests", tableName)).isEmpty();
    }

    @Test
    void testCreateRejectsNonEmptyLocationWithoutChangingStorage()
            throws Exception
    {
        String tableName = "create_at_non_empty_location";
        Location tableLocation = initializer.getExternalLocation().appendPath(tableName);
        Location existingFile = tableLocation.appendPath("existing.txt");
        initializer.getFileSystem().newOutputFile(existingFile).createOrOverwrite(new byte[] {1});

        assertQueryFails(
                "CREATE TABLE " + tableName + " (id bigint) WITH (location = '" + tableLocation + "')",
                ".*already contains files.*Use the register_table procedure.*");

        assertThat(initializer.getFileSystem().newInputFile(existingFile).exists()).isTrue();
        assertThat(HudiUtil.hudiMetadataExists(initializer.getFileSystem(), tableLocation)).isFalse();
        assertThat(initializer.getMetastore().getTable("tests", tableName)).isEmpty();
    }

    @Test
    void testCreatePersistsAdvancedTableProperties()
    {
        String tableName = "created_with_advanced_properties";
        Location tableLocation = initializer.getExternalLocation().appendPath(tableName);

        assertUpdate("""
                CREATE TABLE %s (
                    id bigint,
                    event_time timestamp(6),
                    city varchar
                )
                WITH (
                    location = '%s',
                    table_type = 'MERGE_ON_READ',
                    partitioned_by = ARRAY['city'],
                    primary_key = ARRAY['id'],
                    ordering_fields = ARRAY['event_time'],
                    record_merge_mode = 'EVENT_TIME_ORDERING',
                    key_generator_class = 'org.apache.hudi.keygen.SimpleKeyGenerator',
                    hive_style_partitioning = false,
                    hoodie_properties = MAP(
                        ARRAY['hoodie.keygen.timebased.timestamp.type'],
                        ARRAY['EPOCHMILLISECONDS'])
                )
                """.formatted(tableName, tableLocation));

        assertThat(initializer.loadMetaClient(tableName, tableLocation).getTableConfig())
                .satisfies(config -> {
                    assertThat(config.getRecordMergeMode()).isEqualTo(RecordMergeMode.EVENT_TIME_ORDERING);
                    assertThat(config.getKeyGeneratorClassName())
                            .isEqualTo("org.apache.hudi.keygen.SimpleKeyGenerator");
                    assertThat(config.getProps())
                            .containsEntry(HoodieTableConfig.KEY_GENERATOR_TYPE.key(), KeyGeneratorType.SIMPLE.name())
                            .containsEntry(HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE.key(), "false")
                            .containsEntry("hoodie.keygen.timebased.timestamp.type", "EPOCHMILLISECONDS");
                });

        assertUpdate("DROP TABLE " + tableName);
        assertThat(HudiUtil.hudiMetadataExists(initializer.getFileSystem(), tableLocation)).isTrue();
    }

    @Test
    void testCreateRejectsHoodiePropertyThatWouldNotBePersisted()
    {
        String tableName = "created_with_invalid_hoodie_property";
        Location tableLocation = initializer.getExternalLocation().appendPath(tableName);

        assertQueryFails("""
                CREATE TABLE %s (id bigint)
                WITH (
                    location = '%s',
                    hoodie_properties = MAP(ARRAY['hoodie.unsupported'], ARRAY['value'])
                )
                """.formatted(tableName, tableLocation),
                ".*Hudi does not persist 'hoodie.unsupported' in hoodie.properties.*");

        assertThat(HudiUtil.hudiMetadataExists(initializer.getFileSystem(), tableLocation)).isFalse();
        assertThat(initializer.getMetastore().getTable("tests", tableName)).isEmpty();
    }

    @Test
    void testCreateAndDropExternalMergeOnReadTablePreservesStorage()
    {
        String tableName = "created_external_mor";
        Location tableLocation = initializer.getExternalLocation().appendPath(tableName);

        assertUpdate("""
                CREATE TABLE %s (
                    id bigint,
                    name varchar,
                    city varchar
                )
                WITH (
                    location = '%s',
                    table_type = 'MERGE_ON_READ',
                    partitioned_by = ARRAY['city'],
                    primary_key = ARRAY['id'],
                    ordering_fields = ARRAY['id']
                )
                """.formatted(tableName, tableLocation));

        assertQuery("SELECT count(*) FROM " + tableName, "VALUES CAST(0 AS BIGINT)");
        assertThat(initializer.getMetastore().getTable("tests", tableName)).get()
                .satisfies(table -> {
                    assertThat(table.getTableType()).isEqualTo("EXTERNAL_TABLE");
                    assertThat(table.getStorage().getLocation()).isEqualTo(tableLocation.toString());
                    assertThat(table.getPartitionColumns()).extracting(io.trino.metastore.Column::getName)
                            .containsExactly("city");
                });
        assertThat(initializer.loadMetaClient(tableName, tableLocation).getTableConfig())
                .satisfies(config -> {
                    assertThat(config.getTableType()).isEqualTo(MERGE_ON_READ);
                    assertThat(config.getTableVersion()).isEqualTo(HudiTableInitializer.CREATED_TABLE_VERSION);
                    assertThat(config.getPartitionFields().get()).containsExactly("city");
                    assertThat(config.getRecordKeyFields().get()).containsExactly("id");
                    assertThat(config.getOrderingFields()).containsExactly("id");
                    assertThat(config.getProps()).containsEntry(
                            HoodieTableConfig.KEY_GENERATOR_TYPE.key(), KeyGeneratorType.SIMPLE.name());
                    assertThat(config.getProps()).containsEntry(
                            HoodieTableConfig.HIVE_STYLE_PARTITIONING_ENABLE.key(), "true");
                });

        assertUpdate("DROP TABLE " + tableName);

        assertThat(initializer.getMetastore().getTable("tests", tableName)).isEmpty();
        assertThat(HudiUtil.hudiMetadataExists(initializer.getFileSystem(), tableLocation)).isTrue();
    }

    @Test
    void testCreateAndDropManagedCopyOnWriteTableDeletesStorage()
    {
        String tableName = "created_managed_cow";
        String schemaLocation = initializer.getMetastore().getDatabase("tests").orElseThrow().getLocation().orElseThrow();
        Location tableLocation = Location.of(schemaLocation).appendPath(tableName);

        assertUpdate("CREATE TABLE " + tableName + " (id bigint, name varchar)");

        assertQuery("SELECT count(*) FROM " + tableName, "VALUES CAST(0 AS BIGINT)");
        assertThat(initializer.getMetastore().getTable("tests", tableName)).get()
                .satisfies(table -> {
                    assertThat(table.getTableType()).isEqualTo("MANAGED_TABLE");
                    assertThat(table.getParameters()).doesNotContainKey("EXTERNAL");
                    assertThat(table.getStorage().getLocation()).isEqualTo(tableLocation.toString());
                });
        assertThat(initializer.loadMetaClient(tableName, tableLocation).getTableConfig())
                .satisfies(config -> {
                    assertThat(config.getTableType()).isEqualTo(COPY_ON_WRITE);
                    assertThat(config.getTableVersion()).isEqualTo(HudiTableInitializer.CREATED_TABLE_VERSION);
                    assertThat(config.getProps()).containsEntry(
                            HoodieTableConfig.KEY_GENERATOR_TYPE.key(), KeyGeneratorType.NON_PARTITION.name());
                });

        assertUpdate("DROP TABLE " + tableName);

        assertThat(initializer.getMetastore().getTable("tests", tableName)).isEmpty();
        assertThat(HudiUtil.hudiMetadataExists(initializer.getFileSystem(), tableLocation)).isFalse();
    }

    @Test
    void testInflightOnlyTableHasNoValidCommit()
    {
        String tableName = "inflight_only_table";
        Location tableLocation = initializer.getExternalLocation().appendPath(tableName);
        assertUpdate("CREATE TABLE " + tableName + " (id bigint) WITH (location = '" + tableLocation + "')");

        HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
                .setConf(new TrinoStorageConfiguration(initializer.getFileSystem()))
                .setBasePath(tableLocation.toString())
                .build();
        metaClient.getActiveTimeline().createNewInstant(
                metaClient.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "001"));

        assertThatThrownBy(() -> getQueryRunner().execute(getSession(), "SELECT * FROM " + tableName))
                .rootCause()
                .hasMessage("Table has no valid commits");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    void testUnregisterManagedTablePreservesStorageAndReregistersAsExternal()
    {
        String tableName = "unregister_managed";
        String schemaLocation = initializer.getMetastore().getDatabase("tests").orElseThrow().getLocation().orElseThrow();
        Location tableLocation = Location.of(schemaLocation).appendPath(tableName);
        assertUpdate("CREATE TABLE " + tableName + " (id bigint)");

        assertUpdate("CALL hudi.system.unregister_table('tests', '" + tableName + "')");

        assertThat(initializer.getMetastore().getTable("tests", tableName)).isEmpty();
        assertThat(HudiUtil.hudiMetadataExists(initializer.getFileSystem(), tableLocation)).isTrue();

        assertUpdate("CALL hudi.system.register_table('tests', '" + tableName + "', '" + tableLocation + "')");
        assertThat(initializer.getMetastore().getTable("tests", tableName)).get()
                .satisfies(table -> assertThat(table.getTableType()).isEqualTo("EXTERNAL_TABLE"));
        assertUpdate("DROP TABLE " + tableName);
        assertThat(HudiUtil.hudiMetadataExists(initializer.getFileSystem(), tableLocation)).isTrue();
    }

    private static final class UnregisteredTableInitializer
            implements HudiTablesInitializer
    {
        private final String tableName = "unregistered_v8_table";
        private Location tableLocation;
        private Location externalLocation;
        private TrinoFileSystem fileSystem;
        private HiveMetastore metastore;

        @Override
        public void initializeTables(QueryRunner queryRunner, Location externalLocation, String schemaName)
                throws Exception
        {
            HudiConnector connector = (HudiConnector) queryRunner.getCoordinator().getConnector("hudi");
            fileSystem = connector.getInjector()
                    .getInstance(TrinoFileSystemFactory.class)
                    .create(ConnectorIdentity.ofUser("test"));
            metastore = HudiQueryRunner.getMetastore(queryRunner);
            this.externalLocation = externalLocation;
            tableLocation = externalLocation.appendPath(tableName);

            initializeTable(tableName, tableLocation, schemaName, HoodieFileFormat.PARQUET);
        }

        private void initializeTable(String tableName, Location tableLocation, String schemaName, HoodieFileFormat baseFileFormat)
                throws Exception
        {
            HoodieSchema userSchema = HoodieSchema.createRecord(
                    tableName,
                    "hoodie.trino.test",
                    null,
                    Arrays.asList(
                            HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG)),
                            HoodieSchemaField.of("city", HoodieSchema.create(HoodieSchemaType.STRING))));
            HoodieSchema tableSchema = HoodieSchemaUtils.addMetadataFields(userSchema);

            HoodieTableMetaClient.newTableBuilder()
                    .setTableType(HoodieTableType.COPY_ON_WRITE)
                    .setTableName(tableName)
                    .setDatabaseName(schemaName)
                    .setTableVersion(HoodieTableVersion.EIGHT)
                    .setBaseFileFormat(baseFileFormat.name())
                    .setPartitionFields("city")
                    .setTableCreateSchema(tableSchema.toAvroSchema().toString())
                    .initTable(new TrinoStorageConfiguration(fileSystem), new StoragePath(tableLocation.toString()));
        }

        public String getTableName()
        {
            return tableName;
        }

        public Location getTableLocation()
        {
            return tableLocation;
        }

        public Location getExternalLocation()
        {
            return externalLocation;
        }

        public TrinoFileSystem getFileSystem()
        {
            return fileSystem;
        }

        public HiveMetastore getMetastore()
        {
            return metastore;
        }

        public HoodieTableMetaClient loadMetaClient()
        {
            return HudiUtil.buildTableMetaClient(fileSystem, tableName, tableLocation.toString());
        }

        public HoodieTableMetaClient loadMetaClient(String tableName, Location tableLocation)
        {
            return HudiUtil.buildTableMetaClient(fileSystem, tableName, tableLocation.toString());
        }
    }
}
