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
package io.trino.plugin.hudi.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.Column;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.StorageFormat;
import io.trino.metastore.Table;
import io.trino.plugin.hudi.HudiConnector;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.QueryRunner;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.bootstrap.index.NoOpBootstrapIndex;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.ComplexAvroKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.hive.formats.HiveClassNames.HUDI_PARQUET_INPUT_FORMAT;
import static io.trino.hive.formats.HiveClassNames.MAPRED_PARQUET_OUTPUT_FORMAT_CLASS;
import static io.trino.hive.formats.HiveClassNames.PARQUET_HIVE_SERDE_CLASS;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hudi.testing.HudiTestUtils.hiveSyncStorageSchema;
import static io.trino.plugin.hudi.testing.HudiTestUtils.metastoreDataColumns;
import static java.nio.file.Files.createTempDirectory;

/**
 * Creates a non-partitioned COW table keyed on a date and a UTC timestamp-millis column, with the record-level index
 * in its metadata table. Each record key is built by Hudi's own {@link ComplexAvroKeyGenerator}, as a writer builds
 * it, e.g. {@code event_date:2024-01-02,event_ts:1704189600123}. Each commit inserts one row into a new file group
 * (small-file packing is off), so a lookup that finds one key reads one split:
 * <pre>
 * e1: event_date 2024-01-01, event_ts 2024-01-01 10:00:00.123 UTC, name a
 * e2: event_date 2024-01-02, event_ts 2024-01-02 10:00:00.123 UTC, name b
 * e3: event_date 2024-01-03, event_ts 2024-01-03 10:00:00.123 UTC, name c
 * </pre>
 */
public class LogicalTypeRecordKeyHudiTablesInitializer
        implements HudiTablesInitializer
{
    public static final String TABLE_NAME = "hudi_logical_type_key_rli_cow";

    private static final String ORDERING_FIELD = "ts";
    private static final List<String> RECORD_KEY_FIELDS = ImmutableList.of("event_date", "event_ts");
    // 19723 is 2024-01-01; 1704103200123 ms is 2024-01-01 10:00:00.123 UTC
    private static final int FIRST_EPOCH_DAY = 19_723;
    private static final long FIRST_EPOCH_MILLIS = 1_704_103_200_123L;
    private static final long MILLIS_PER_DAY = 86_400_000L;

    private static final Schema AVRO_SCHEMA = HoodieSchema.createRecord(TABLE_NAME, null, null, ImmutableList.of(
            HoodieSchemaField.of("event_date", HoodieSchema.createDate()),
            HoodieSchemaField.of("event_ts", HoodieSchema.createTimestampMillis()),
            HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of(ORDERING_FIELD, HoodieSchema.create(HoodieSchemaType.LONG)))).toAvroSchema();

    @Override
    public void initializeTables(QueryRunner queryRunner, Location externalLocation, String schemaName)
            throws Exception
    {
        TrinoFileSystem fileSystem = ((HudiConnector) queryRunner.getCoordinator().getConnector("hudi")).getInjector()
                .getInstance(TrinoFileSystemFactory.class)
                .create(ConnectorIdentity.ofUser("test"));
        HiveMetastore metastore = ((HudiConnector) queryRunner.getCoordinator().getConnector("hudi")).getInjector()
                .getInstance(HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());

        java.nio.file.Path tempDir = createTempDirectory("logical-type-key-rli");
        try {
            java.nio.file.Path tableDir = tempDir.resolve(TABLE_NAME);
            Path tablePath = new Path(tableDir.toUri());
            writeTable(tablePath);
            HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
                    .setConf(new HadoopStorageConfiguration(new Configuration()))
                    .setBasePath(tablePath.toString())
                    .build();
            List<Column> dataColumns = metastoreDataColumns(hiveSyncStorageSchema(metaClient, false), false);
            Location tableLocation = externalLocation.appendPath(TABLE_NAME);
            ResourceHudiTablesInitializer.copyDir(tableDir, fileSystem, tableLocation);
            metastore.createTable(createTableDefinition(schemaName, tableLocation, dataColumns), PrincipalPrivileges.NO_PRIVILEGES);
        }
        finally {
            deleteRecursively(tempDir, ALLOW_INSECURE);
        }
    }

    private static void writeTable(Path tablePath)
            throws IOException
    {
        HoodieTableMetaClient.newTableBuilder()
                .setTableType(HoodieTableType.COPY_ON_WRITE)
                .setTableName(TABLE_NAME)
                .setTimelineLayoutVersion(1)
                .setBootstrapIndexClass(NoOpBootstrapIndex.class.getName())
                .setPayloadClassName(HoodieAvroPayload.class.getName())
                .setRecordKeyFields(String.join(",", RECORD_KEY_FIELDS))
                .setKeyGeneratorClassProp(ComplexAvroKeyGenerator.class.getName())
                .setOrderingFields(ORDERING_FIELD)
                .initTable(new HadoopStorageConfiguration(new Configuration()), tablePath.toString());

        TypedProperties keyGeneratorProps = new TypedProperties();
        keyGeneratorProps.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), String.join(",", RECORD_KEY_FIELDS));
        keyGeneratorProps.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "");
        ComplexAvroKeyGenerator keyGenerator = new ComplexAvroKeyGenerator(keyGeneratorProps);

        try (HoodieJavaWriteClient<HoodieAvroPayload> writeClient = createWriteClient(tablePath)) {
            List<String> names = ImmutableList.of("a", "b", "c");
            for (int day = 0; day < names.size(); day++) {
                GenericRecord record = new GenericData.Record(AVRO_SCHEMA);
                record.put("event_date", FIRST_EPOCH_DAY + day);
                record.put("event_ts", FIRST_EPOCH_MILLIS + day * MILLIS_PER_DAY);
                record.put("name", names.get(day));
                record.put(ORDERING_FIELD, 100L);
                HoodieRecord<HoodieAvroPayload> hoodieRecord = new HoodieAvroRecord<>(keyGenerator.getKey(record), new HoodieAvroPayload(Option.of(record)), null);
                String commit = writeClient.startCommit();
                writeClient.commit(commit, writeClient.insert(ImmutableList.of(hoodieRecord), commit));
            }
        }
    }

    private static HoodieJavaWriteClient<HoodieAvroPayload> createWriteClient(Path tablePath)
    {
        HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
                .withPath(tablePath.toString())
                .withSchema(AVRO_SCHEMA.toString())
                .withParallelism(2, 2)
                .withDeleteParallelism(2)
                .forTable(TABLE_NAME)
                .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
                // No small-file packing: every insert opens a new file group
                .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(0).build())
                .withEmbeddedTimelineServerEnabled(false)
                .withMarkersType(MarkerType.DIRECT.name())
                .withMetadataConfig(HoodieMetadataConfig.newBuilder()
                        .enable(true)
                        .withEnableGlobalRecordLevelIndex(true)
                        .withMetadataIndexColumnStats(false)
                        .withMetadataIndexPartitionStats(false)
                        .build())
                .build();
        return new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(new HadoopStorageConfiguration(new Configuration())), config);
    }

    private static Table createTableDefinition(String schemaName, Location location, List<Column> dataColumns)
    {
        return Table.builder()
                .setDatabaseName(schemaName)
                .setTableName(TABLE_NAME)
                .setTableType(EXTERNAL_TABLE.name())
                .setOwner(Optional.of("public"))
                .setDataColumns(dataColumns)
                .setParameters(ImmutableMap.of("serialization.format", "1", "EXTERNAL", "TRUE"))
                .withStorage(storageBuilder -> storageBuilder
                        .setStorageFormat(StorageFormat.create(PARQUET_HIVE_SERDE_CLASS, HUDI_PARQUET_INPUT_FORMAT, MAPRED_PARQUET_OUTPUT_FORMAT_CLASS))
                        .setLocation(location.toString()))
                .build();
    }
}
