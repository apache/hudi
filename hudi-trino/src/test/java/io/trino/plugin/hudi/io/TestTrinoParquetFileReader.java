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
package io.trino.plugin.hudi.io;

import com.google.common.io.Resources;
import io.trino.filesystem.local.LocalFileSystem;
import io.trino.plugin.hudi.storage.HudiTrinoStorage;
import io.trino.plugin.hudi.storage.TrinoStorageConfiguration;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.avro.model.HoodieLSMTimelineInstant;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.LSMTimeline;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests {@link TrinoParquetFileReader} against a four-instant LSM archived-timeline parquet file, the shape Hudi's
 * archived-timeline loader reads through the connector. {@code archived_timeline.parquet} is the history file
 * {@code 20250918121953134_20250918122001506_0.parquet} of a table-version-8 COW table written by Hudi 1.0.2 (four
 * commit instants, 20250918121953134 through 20250918122001506), generated with the create script documented in
 * {@code hudi-testing-data/hudi_mor_archived_timeline.md}, COW variant.
 */
class TestTrinoParquetFileReader
{
    private static final String ARCHIVED_TIMELINE_PARQUET_FILE = "archived_timeline.parquet";

    @Test
    void testReadArchivedTimelineFile()
            throws Exception
    {
        HoodieSchema tableSchema = HoodieSchema.fromAvroSchema(HoodieLSMTimelineInstant.getClassSchema());
        Schema avroSchema = tableSchema.toAvroSchema();

        try (TrinoParquetFileReader reader = createReader()) {
            assertThat(reader.getSchema().toAvroSchema()).isEqualTo(HoodieLSMTimelineInstant.getClassSchema());
            assertThat(reader.getTotalRecords()).isEqualTo(4);

            List<IndexedRecord> records = new ArrayList<>();
            try (ClosableIterator<IndexedRecord> iterator = reader.getIndexedRecordIterator(tableSchema, tableSchema)) {
                iterator.forEachRemaining(records::add);
            }
            assertThat(records).hasSize(4);

            IndexedRecord firstRecord = records.getFirst();
            assertThat(firstRecord.get(avroSchema.getField("instantTime").pos()).toString()).isEqualTo("20250918121953134");
            assertThat(firstRecord.get(avroSchema.getField("completionTime").pos()).toString()).isEqualTo("20250918121957816");
            assertThat(firstRecord.get(avroSchema.getField("action").pos()).toString()).isEqualTo("commit");

            IndexedRecord secondRecord = records.get(1);
            assertThat(secondRecord.get(avroSchema.getField("instantTime").pos()).toString()).isEqualTo("20250918121958100");
            assertThat(secondRecord.get(avroSchema.getField("completionTime").pos()).toString()).isEqualTo("20250918121959081");
            assertThat(secondRecord.get(avroSchema.getField("action").pos()).toString()).isEqualTo("commit");

            IndexedRecord lastRecord = records.getLast();
            assertThat(lastRecord.get(avroSchema.getField("instantTime").pos()).toString()).isEqualTo("20250918122001506");
            assertThat(lastRecord.get(avroSchema.getField("completionTime").pos()).toString()).isEqualTo("20250918122002218");
        }
    }

    @ParameterizedTest
    @EnumSource(value = HoodieArchivedTimeline.LoadMode.class, names = {"TIME", "FULL"})
    void testProjectedReadUsesRequestedSchema(HoodieArchivedTimeline.LoadMode loadMode)
            throws Exception
    {
        // The projections ArchivedTimelineLoaderV2 requests: TIME when it only needs instant times, FULL when it needs
        // the payloads too. FULL is the interesting one -- its read schema orders plan before metadata, the reverse of
        // the file's metadata, plan, so a passing read proves columns are mapped by name and not by position, and it is
        // the only projection that materializes the bytes columns the SqlVarbinary -> ByteBuffer conversion exists for
        Schema projectedAvroSchema = LSMTimeline.getReadSchema(loadMode);
        HoodieSchema tableSchema = HoodieSchema.fromAvroSchema(HoodieLSMTimelineInstant.getClassSchema());
        HoodieSchema projectedSchema = HoodieSchema.fromAvroSchema(projectedAvroSchema);

        try (TrinoParquetFileReader reader = createReader()) {
            List<IndexedRecord> records = new ArrayList<>();
            try (ClosableIterator<IndexedRecord> iterator = reader.getIndexedRecordIterator(tableSchema, projectedSchema)) {
                iterator.forEachRemaining(records::add);
            }
            assertThat(records).hasSize(4);
            assertThat(records).allSatisfy(record -> assertThat(record.getSchema()).isEqualTo(projectedAvroSchema));

            int instantTimePos = projectedAvroSchema.getField("instantTime").pos();
            int completionTimePos = projectedAvroSchema.getField("completionTime").pos();
            assertThat(records.getFirst().get(instantTimePos).toString()).isEqualTo("20250918121953134");
            assertThat(records.getFirst().get(completionTimePos).toString()).isEqualTo("20250918121957816");
            assertThat(records.get(1).get(instantTimePos).toString()).isEqualTo("20250918121958100");
            assertThat(records.get(1).get(completionTimePos).toString()).isEqualTo("20250918121959081");

            if (projectedAvroSchema.getField("metadata") != null) {
                int metadataPos = projectedAvroSchema.getField("metadata").pos();
                int planPos = projectedAvroSchema.getField("plan").pos();
                // All four instants of the fixture are commits: every row has metadata bytes and a null plan
                assertThat(records).allSatisfy(record -> {
                    assertThat(record.get(metadataPos)).isInstanceOf(ByteBuffer.class);
                    assertThat(((ByteBuffer) record.get(metadataPos)).remaining()).isGreaterThan(0);
                    assertThat(record.get(planPos)).isNull();
                });
            }
        }
    }

    @Test
    void testFooterLookupsUnsupported()
            throws Exception
    {
        // A timeline file is not a data file: it carries neither a bloom filter nor min/max record keys
        try (TrinoParquetFileReader reader = createReader()) {
            assertThatThrownBy(reader::readBloomFilter).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(reader::readMinMaxRecordKeys).isInstanceOf(UnsupportedOperationException.class);
        }
    }

    private static TrinoParquetFileReader createReader()
            throws Exception
    {
        File parquetFile = new File(Resources.getResource(ARCHIVED_TIMELINE_PARQUET_FILE).toURI());
        HoodieStorage storage = new HudiTrinoStorage(new LocalFileSystem(Paths.get("/")), new TrinoStorageConfiguration());
        return new TrinoParquetFileReader(storage, new StoragePath(parquetFile.toURI().toString()));
    }
}
