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
import io.trino.metastore.Column;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.config.HoodieWriteConfig;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Creates a non-partitioned Merge-On-Read table at TABLE VERSION 6 whose only merge configuration is a
 * {@link org.apache.hudi.common.model.HoodieRecordPayload} class ({@link RankBasedTestPayload}), the way a
 * genuine pre-1.0 table looks on storage.
 * <p>
 * Such a table resolves at read time to {@code CUSTOM} merge mode with the payload-based merge strategy, which
 * in turn resolves {@code HoodieAvroRecordMerger}. That merger is not projection compatible, so the file group
 * reader demands the FULL table schema for base and log reads alike and nothing prepends the payload's decision
 * column into the connector's read projection.
 * <p>
 * One {@code bulkInsert} (base files) is followed by one {@code upsert} of the same keys (log files), with data
 * laid out so each merge direction is distinguishable: {@code k1}'s winning rank is on the LOG record and
 * {@code k2}'s on the BASE record. See {@code TestHudiNonProjectionCompatibleMerger}.
 */
public class PayloadOnlyMergerHudiTablesInitializer
        extends AbstractMergerHudiTablesInitializer
{
    public static final String TABLE_NAME = "payload_only_mor";
    public static final String RT_TABLE_NAME = TABLE_NAME + "_rt";

    private static final String RANK_FIELD = RankBasedTestPayload.RANK_COLUMN;
    private static final HoodieTableVersion TABLE_VERSION = HoodieTableVersion.SIX;

    public PayloadOnlyMergerHudiTablesInitializer()
    {
        super(TABLE_NAME);
    }

    @Override
    protected List<Column> dataColumns()
    {
        return ImmutableList.of(
                new Column(RECORD_KEY_FIELD, HIVE_STRING, Optional.empty(), Map.of()),
                new Column("name", HIVE_STRING, Optional.empty(), Map.of()),
                new Column("value", HIVE_LONG, Optional.empty(), Map.of()),
                new Column(RANK_FIELD, HIVE_LONG, Optional.empty(), Map.of()),
                new Column(ORDERING_FIELD, HIVE_LONG, Optional.empty(), Map.of()));
    }

    @Override
    protected Schema avroSchema()
    {
        List<Schema.Field> fields = ImmutableList.of(
                new Schema.Field(RECORD_KEY_FIELD, Schema.create(Schema.Type.STRING)),
                new Schema.Field("name", Schema.create(Schema.Type.STRING)),
                new Schema.Field("value", Schema.create(Schema.Type.LONG)),
                new Schema.Field(RANK_FIELD, Schema.create(Schema.Type.LONG)),
                new Schema.Field(ORDERING_FIELD, Schema.create(Schema.Type.LONG)));
        return Schema.createRecord(TABLE_NAME, null, null, false, new ArrayList<>(fields));
    }

    @Override
    protected void configureTableConfig(HoodieTableMetaClient.TableBuilder tableBuilder)
    {
        // Only the payload class: the merge mode and the merge strategy id must stay out of hoodie.properties
        // so the reader has to infer them, which is what makes this a pre-1.0 payload-only table.
        tableBuilder
                .setTableVersion(TABLE_VERSION)
                .setPayloadClassName(RankBasedTestPayload.class.getName());
    }

    @Override
    protected void configureWriteConfig(HoodieWriteConfig.Builder writeConfigBuilder)
    {
        writeConfigBuilder
                .withWriteTableVersion(TABLE_VERSION.versionCode())
                // Without this the write client silently upgrades the table to the current version on the first commit.
                .withAutoUpgradeVersion(false)
                .withWritePayLoad(RankBasedTestPayload.class.getName());
    }

    @Override
    protected void afterTableInit()
            throws IOException
    {
        stripInferredMergeStrategyId();
    }

    @Override
    protected void writeInitialCommits(HoodieJavaWriteClient<HoodieAvroPayload> client)
            throws IOException
    {
        Schema schema = avroSchema();
        // First commit: bulk insert base records (produces base parquet files).
        String firstCommit = client.startCommit();
        List<WriteStatus> firstStatuses = client.bulkInsert(ImmutableList.of(
                record(schema, "k1", "k1_base", 10L, 5L, 1L),
                record(schema, "k2", "k2_base", 100L, 9L, 1L)), firstCommit);
        client.commit(firstCommit, firstStatuses);

        // Second commit: upserts the same keys (produces log files since inline compaction is disabled).
        // k1 update has a HIGHER rank (7 > 5) -> the payload keeps the update (99): the LOG record's rank decides.
        // k2 update has a LOWER rank (1 < 9) -> the payload keeps the base record (100): the BASE record's rank
        // decides, which only works when the base read carries merge_rank despite it never being projected.
        String secondCommit = client.startCommit();
        List<WriteStatus> secondStatuses = client.upsert(ImmutableList.of(
                record(schema, "k1", "k1_updated", 99L, 7L, 2L),
                record(schema, "k2", "k2_updated", 4L, 1L, 2L)), secondCommit);
        client.commit(secondCommit, secondStatuses);

        // The commits go through the table config; re-check that the writer left the fixture payload-only.
        stripInferredMergeStrategyId();
    }

    /**
     * Removes the merge strategy id that table creation infers and persists even for a version 6 table,
     * then verifies what is left. The merge MODE never reaches disk here: its config is since-version
     * 1.0.0, so creation itself drops it for a version 6 table ({@code HoodieTableConfig.dropInvalidConfigs});
     * the {@code checkAbsent} below just pins that. The fixture has to mimic a genuine pre-1.0 table, which
     * persists its payload class and nothing else about merging; leaving the inferred id in would hand the
     * reader the answer this fixture exists to make it derive.
     */
    private void stripInferredMergeStrategyId()
            throws IOException
    {
        Path metaDirectory = stagingTableDirectory().resolve(".hoodie");
        Path propertiesFile = metaDirectory.resolve("hoodie.properties");
        List<String> retainedLines = Files.readAllLines(propertiesFile, UTF_8).stream()
                .filter(line -> !isEntryFor(line, HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key()))
                .toList();
        Files.write(propertiesFile, retainedLines, UTF_8);
        // The Hadoop local filesystem verifies this sidecar when the write client reopens the file, and an
        // out-of-band edit leaves it stale; dropping it makes the file checksum-free rather than corrupt.
        Files.deleteIfExists(metaDirectory.resolve(".hoodie.properties.crc"));

        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(propertiesFile)) {
            properties.load(input);
        }
        checkProperty(properties, HoodieTableConfig.VERSION.key(), String.valueOf(TABLE_VERSION.versionCode()));
        checkProperty(properties, HoodieTableConfig.PAYLOAD_CLASS_NAME.key(), RankBasedTestPayload.class.getName());
        checkAbsent(properties, HoodieTableConfig.RECORD_MERGE_MODE.key());
        checkAbsent(properties, HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key());
    }

    private static boolean isEntryFor(String line, String key)
    {
        return line.startsWith(key + "=") || line.startsWith(key + ":") || line.startsWith(key + " ");
    }

    private static void checkProperty(Properties properties, String key, String expected)
    {
        String actual = properties.getProperty(key);
        if (!expected.equals(actual)) {
            throw new IllegalStateException("Expected %s=%s in hoodie.properties of %s but found %s".formatted(key, expected, TABLE_NAME, actual));
        }
    }

    private static void checkAbsent(Properties properties, String key)
    {
        if (properties.containsKey(key)) {
            throw new IllegalStateException("Expected no %s in hoodie.properties of %s but found %s".formatted(key, TABLE_NAME, properties.getProperty(key)));
        }
    }

    private static HoodieRecord<HoodieAvroPayload> record(Schema schema, String key, String name, long value, long rank, long ts)
    {
        GenericRecord record = new GenericData.Record(schema);
        record.put(RECORD_KEY_FIELD, key);
        record.put("name", name);
        record.put("value", value);
        record.put(RANK_FIELD, rank);
        record.put(ORDERING_FIELD, ts);
        return avroRecord(record, key);
    }
}
