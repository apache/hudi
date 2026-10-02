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

import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.metastore.Database;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.Table;
import io.trino.metastore.TableAlreadyExistsException;
import io.trino.plugin.hudi.util.HudiSchemaConverter;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.type.TypeManager;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

import static com.google.common.base.Throwables.getCausalChain;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.trino.plugin.hive.HiveMetadata.TRINO_QUERY_ID_NAME;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hudi.HudiTableProperties.TABLE_TYPE_PROPERTY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;
import static org.apache.hudi.common.util.ConfigUtils.IS_QUERY_AS_RO_TABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestHudiMetadata
{
    @TempDir
    private Path temporaryDirectory;

    @Test
    void testCreateTableRaceDoesNotDeleteWinningTableMetadata()
    {
        CreateRace race = createRace(TestHudiMetadata::asTableCreatedByAnotherQuery, false);

        assertThatThrownBy(() -> race.metadata().createTable(SESSION, race.tableMetadata(), SaveMode.FAIL))
                .isInstanceOf(TableAlreadyExistsException.class);

        assertThat(race.winningTable().get()).isNotNull();
        assertThat(metadataExists(race, race.winningTable().get()))
                .isTrue();
    }

    @Test
    void testCreateTableRetryRecognizesCommittedTableAfterLocationNormalization()
    {
        CreateRace race = createRace(table -> Table.builder(table)
                .withStorage(storage -> storage.setLocation(
                        table.getStorage().getLocation().replace("local:///", "local:/")))
                .build(), false);

        assertThatCode(() -> race.metadata().createTable(SESSION, race.tableMetadata(), SaveMode.FAIL))
                .doesNotThrowAnyException();

        assertThat(race.attemptedTable().get()).isNotNull();
        assertThat(race.attemptedTable().get().getParameters())
                .containsEntry(TRINO_QUERY_ID_NAME, SESSION.getQueryId())
                .containsEntry(HudiMetadata.TRINO_MANAGED_TABLE_PARAMETER, "true");
        assertThat(metadataExists(race, race.attemptedTable().get()))
                .isTrue();
    }

    @Test
    void testCreateTableRacePreservesMetadataWhenWinnerUsesDifferentLocation()
    {
        CreateRace race = createRace(table -> Table.builder(table)
                .setParameter(TRINO_QUERY_ID_NAME, "another_query")
                .withStorage(storage -> storage.setLocation("local:///other_table"))
                .build(), false);

        assertThatThrownBy(() -> race.metadata().createTable(SESSION, race.tableMetadata(), SaveMode.FAIL))
                .isInstanceOf(TableAlreadyExistsException.class);

        assertThat(race.attemptedTable().get()).isNotNull();
        assertThat(race.winningTable().get()).isNotNull();
        assertThat(race.winningTable().get().getStorage().getLocation())
                .isNotEqualTo(race.attemptedTable().get().getStorage().getLocation());
        assertThat(metadataExists(race, race.attemptedTable().get()))
                .isTrue();
    }

    @Test
    void testCreateTableRacePreservesMetadataWhenWinnerLookupFails()
    {
        CreateRace race = createRace(TestHudiMetadata::asTableCreatedByAnotherQuery, true);

        assertThatThrownBy(() -> race.metadata().createTable(SESSION, race.tableMetadata(), SaveMode.FAIL))
                .isInstanceOf(TableAlreadyExistsException.class)
                .satisfies(failure -> assertThat(failure.getSuppressed()).hasSize(1));

        assertThat(race.attemptedTable().get()).isNotNull();
        assertThat(metadataExists(race, race.attemptedTable().get()))
                .isTrue();
    }

    @Test
    void testCreateTableFailureCleansMetadataWhenNoTableWasRegistered()
    {
        CreateRace race = createRace(UnaryOperator.identity(), false, false);

        assertThatThrownBy(() -> race.metadata().createTable(SESSION, race.tableMetadata(), SaveMode.FAIL))
                .isInstanceOf(TableAlreadyExistsException.class);

        assertThat(race.attemptedTable().get()).isNotNull();
        assertThat(race.winningTable().get()).isNull();
        assertThat(metadataExists(race, race.attemptedTable().get()))
                .isFalse();
    }

    @Test
    void testCreateTableInitializationConflictPreservesExistingMetadata()
    {
        SchemaTableName tableName = new SchemaTableName("test_schema", "concurrent_create");
        ConnectorTableMetadata tableMetadata = createTableMetadata(tableName);
        String basePath = "local:///test_schema/concurrent_create";
        LocalFileSystemFactory localFileSystemFactory = new LocalFileSystemFactory(temporaryDirectory);
        TrinoFileSystem localFileSystem = localFileSystemFactory.create(SESSION);
        HoodieSchema tableSchema = HudiSchemaConverter.toTableSchema(tableMetadata.getColumns(), tableName.getTableName());
        AtomicBoolean winnerInitialized = new AtomicBoolean();
        TrinoFileSystem racingFileSystem = (TrinoFileSystem) Proxy.newProxyInstance(
                TrinoFileSystem.class.getClassLoader(),
                new Class<?>[] {TrinoFileSystem.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("listFiles") && winnerInitialized.compareAndSet(false, true)) {
                        HudiTableInitializer.initializeTable(localFileSystem, basePath, tableMetadata, tableSchema);
                        return FileIterator.empty();
                    }
                    try {
                        return method.invoke(localFileSystem, arguments);
                    }
                    catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
        TrinoFileSystemFactory racingFileSystemFactory = identity -> racingFileSystem;
        HiveMetastore metastore = (HiveMetastore) Proxy.newProxyInstance(
                HiveMetastore.class.getClassLoader(),
                new Class<?>[] {HiveMetastore.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getDatabase" -> Optional.of(Database.builder()
                            .setDatabaseName(tableName.getSchemaName())
                            .setLocation(Optional.of("local:///test_schema"))
                            .setOwnerName(Optional.of("public"))
                            .setOwnerType(Optional.of(PrincipalType.ROLE))
                            .build());
                    case "getTable" -> Optional.empty();
                    default -> throw new AssertionError("Unexpected metastore call: " + method);
                });
        HudiMetadata metadata = new HudiMetadata(
                metastore,
                racingFileSystemFactory,
                unusedTypeManager(),
                newDirectExecutorService());

        assertThatThrownBy(() -> metadata.createTable(SESSION, tableMetadata, SaveMode.FAIL))
                .isInstanceOf(TrinoException.class)
                .satisfies(failure -> assertThat(getCausalChain(failure))
                        .anyMatch(FileAlreadyExistsException.class::isInstance));

        assertThat(winnerInitialized).isTrue();
        assertThat(HudiUtil.hudiMetadataExists(localFileSystem, Location.of(basePath)))
                .isTrue();
    }

    @Test
    void testDropTreatsExternalTableTypeAsSufficientToPreserveStorage()
    {
        SchemaTableName tableName = new SchemaTableName("test_schema", "external_by_type");
        ConnectorTableMetadata tableMetadata = createTableMetadata(tableName);
        String basePath = "local:///test_schema/external_by_type";
        LocalFileSystemFactory fileSystemFactory = new LocalFileSystemFactory(temporaryDirectory);
        TrinoFileSystem fileSystem = fileSystemFactory.create(SESSION);
        HoodieSchema tableSchema = HudiSchemaConverter.toTableSchema(
                tableMetadata.getColumns(), tableName.getTableName());
        HudiTableInitializer.initializeTable(fileSystem, basePath, tableMetadata, tableSchema);

        Table table = Table.builder(HudiMetastoreTables.buildTable(
                        tableName,
                        basePath,
                        HoodieTableType.COPY_ON_WRITE,
                        tableSchema,
                        List.of(),
                        false,
                        Optional.empty(),
                        Optional.empty()))
                // Some metastores use only the type signal for external tables.
                .setTableType(EXTERNAL_TABLE.name())
                .build();
        AtomicReference<Boolean> deleteData = new AtomicReference<>();
        HiveMetastore metastore = (HiveMetastore) Proxy.newProxyInstance(
                HiveMetastore.class.getClassLoader(),
                new Class<?>[] {HiveMetastore.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getTable" -> Optional.of(table);
                    case "dropTable" -> {
                        deleteData.set((Boolean) arguments[2]);
                        yield null;
                    }
                    default -> throw new AssertionError("Unexpected metastore call: " + method);
                });
        HudiMetadata metadata = new HudiMetadata(
                metastore,
                fileSystemFactory,
                unusedTypeManager(),
                newDirectExecutorService());
        HudiTableHandle tableHandle = new HudiTableHandle(
                tableName.getSchemaName(),
                tableName.getTableName(),
                basePath,
                HoodieTableType.COPY_ON_WRITE,
                List.of(),
                List.of(),
                TupleDomain.all(),
                TupleDomain.all(),
                OptionalLong.empty(),
                tableSchema.toAvroSchema().toString(),
                "0");

        metadata.dropTable(SESSION, tableHandle);

        assertThat(table.getParameters()).doesNotContainKey("EXTERNAL");
        assertThat(deleteData).hasValue(false);
        assertThat(HudiUtil.hudiMetadataExists(fileSystem, Location.of(basePath))).isTrue();
    }

    @Test
    void testDropManagedHiveSyncViewsPreservesSharedStorage()
            throws IOException
    {
        String basePath = "local:///test_schema/shared_mor";
        LocalFileSystemFactory fileSystemFactory = new LocalFileSystemFactory(temporaryDirectory);
        TrinoFileSystem fileSystem = fileSystemFactory.create(SESSION);
        SchemaTableName physicalName = new SchemaTableName("test_schema", "shared_mor");
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                physicalName, createTableMetadata(physicalName).getColumns(), Map.of(TABLE_TYPE_PROPERTY, MERGE_ON_READ));
        HoodieSchema tableSchema = HudiSchemaConverter.toTableSchema(tableMetadata.getColumns(), physicalName.getTableName());
        HudiTableInitializer.initializeTable(fileSystem, basePath, tableMetadata, tableSchema);
        Location dataFile = Location.of(basePath).appendPath("existing.parquet");
        fileSystem.newOutputFile(dataFile).createOrOverwrite(new byte[] {1});

        Map<String, Table> tables = new HashMap<>();
        for (String viewName : List.of("shared_mor_ro", "shared_mor_rt")) {
            SchemaTableName name = new SchemaTableName("test_schema", viewName);
            Table table = HudiMetastoreTables.buildTable(name, basePath, MERGE_ON_READ, tableSchema,
                    List.of(), false, Optional.empty(), Optional.empty());
            tables.put(viewName, table);
        }
        // Even a copied ownership marker must not let a read-optimized alias delete the base path.
        tables.put("shared_mor_ro", Table.builder(tables.get("shared_mor_ro"))
                .setParameter(HudiMetadata.TRINO_MANAGED_TABLE_PARAMETER, "true")
                .withStorage(storage -> storage.setSerdeParameters(Map.of(IS_QUERY_AS_RO_TABLE, "true")))
                .build());
        tables.put("shared_mor_rt", Table.builder(tables.get("shared_mor_rt"))
                .setParameter(HudiMetadata.TRINO_MANAGED_TABLE_PARAMETER, "true")
                .build());

        AtomicReference<Boolean> deleteData = new AtomicReference<>();
        HiveMetastore metastore = (HiveMetastore) Proxy.newProxyInstance(
                HiveMetastore.class.getClassLoader(),
                new Class<?>[] {HiveMetastore.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getTable" -> Optional.ofNullable(tables.get(arguments[1]));
                    case "dropTable" -> {
                        deleteData.set((Boolean) arguments[2]);
                        tables.remove(arguments[1]);
                        yield null;
                    }
                    default -> throw new AssertionError("Unexpected metastore call: " + method);
                });
        HudiMetadata metadata = new HudiMetadata(metastore, fileSystemFactory, unusedTypeManager(), newDirectExecutorService());

        for (String viewName : List.of("shared_mor_ro", "shared_mor_rt")) {
            metadata.dropTable(SESSION, new HudiTableHandle(
                    "test_schema", viewName, basePath, MERGE_ON_READ, List.of(), List.of(),
                    TupleDomain.all(), TupleDomain.all(), OptionalLong.empty(), tableSchema.toAvroSchema().toString(), "0"));
            assertThat(deleteData).hasValue(false);
            assertThat(HudiUtil.hudiMetadataExists(fileSystem, Location.of(basePath))).isTrue();
            assertThat(fileSystem.newInputFile(dataFile).exists()).isTrue();
            assertThat(tables).doesNotContainKey(viewName);
            if (viewName.equals("shared_mor_ro")) {
                assertThat(tables).containsKey("shared_mor_rt");
            }
        }
    }

    private CreateRace createRace(UnaryOperator<Table> winningTableFactory, boolean failWinnerLookup)
    {
        return createRace(winningTableFactory, failWinnerLookup, true);
    }

    private CreateRace createRace(UnaryOperator<Table> winningTableFactory, boolean failWinnerLookup, boolean registerWinner)
    {
        SchemaTableName tableName = new SchemaTableName("test_schema", "concurrent_create");
        AtomicReference<Table> attemptedTable = new AtomicReference<>();
        AtomicReference<Table> winningTable = new AtomicReference<>();
        HiveMetastore metastore = (HiveMetastore) Proxy.newProxyInstance(
                HiveMetastore.class.getClassLoader(),
                new Class<?>[] {HiveMetastore.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getDatabase" -> Optional.of(Database.builder()
                            .setDatabaseName(tableName.getSchemaName())
                            .setLocation(Optional.of("local:///test_schema"))
                            .setOwnerName(Optional.of("public"))
                            .setOwnerType(Optional.of(PrincipalType.ROLE))
                            .build());
                    case "getTable" -> {
                        if (failWinnerLookup && attemptedTable.get() != null) {
                            throw new IllegalStateException("metastore unavailable during ownership check");
                        }
                        yield Optional.ofNullable(winningTable.get());
                    }
                    case "createTable" -> {
                        Table table = (Table) arguments[0];
                        attemptedTable.set(table);
                        if (registerWinner) {
                            winningTable.set(winningTableFactory.apply(table));
                        }
                        throw new TableAlreadyExistsException(tableName);
                    }
                    default -> throw new AssertionError("Unexpected metastore call: " + method);
                });
        TrinoFileSystemFactory fileSystemFactory = new LocalFileSystemFactory(temporaryDirectory);
        HudiMetadata metadata = new HudiMetadata(
                metastore,
                fileSystemFactory,
                unusedTypeManager(),
                newDirectExecutorService());
        ConnectorTableMetadata tableMetadata = createTableMetadata(tableName);
        return new CreateRace(metadata, fileSystemFactory, tableMetadata, attemptedTable, winningTable);
    }

    private static ConnectorTableMetadata createTableMetadata(SchemaTableName tableName)
    {
        return new ConnectorTableMetadata(
                tableName,
                List.of(ColumnMetadata.builder()
                        .setName("id")
                        .setType(BIGINT)
                        .build()));
    }

    private static TypeManager unusedTypeManager()
    {
        return (TypeManager) Proxy.newProxyInstance(
                TypeManager.class.getClassLoader(),
                new Class<?>[] {TypeManager.class},
                (proxy, method, arguments) -> {
                    throw new AssertionError("Unexpected type manager call: " + method);
                });
    }

    private static Table asTableCreatedByAnotherQuery(Table table)
    {
        return Table.builder(table)
                .setParameter(TRINO_QUERY_ID_NAME, "another_query")
                .build();
    }

    private static boolean metadataExists(CreateRace race, Table table)
    {
        return HudiUtil.hudiMetadataExists(
                race.fileSystemFactory().create(SESSION),
                Location.of(table.getStorage().getLocation()));
    }

    private record CreateRace(
            HudiMetadata metadata,
            TrinoFileSystemFactory fileSystemFactory,
            ConnectorTableMetadata tableMetadata,
            AtomicReference<Table> attemptedTable,
            AtomicReference<Table> winningTable) {}
}
