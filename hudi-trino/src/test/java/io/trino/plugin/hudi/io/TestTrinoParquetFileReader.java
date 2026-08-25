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

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link TrinoParquetFileReader} against a four-instant LSM archived-timeline parquet file, the shape
 * Hudi's archived-timeline loader reads through the connector.
 */
public class TestTrinoParquetFileReader
{
    private static final String ARCHIVED_TIMELINE_PARQUET_FILE = "archived_timeline.parquet";

    @Test
    public void testReadArchivedTimelineFile()
            throws Exception
    {
        HoodieSchema tableSchema = HoodieSchema.fromAvroSchema(HoodieLSMTimelineInstant.getClassSchema());
        Schema avroSchema = tableSchema.toAvroSchema();

        try (TrinoParquetFileReader reader = createReader()) {
            assertThat(reader.getSchema()).isNotNull();
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

    @Test
    public void testProjectedReadUsesRequestedSchema()
            throws Exception
    {
        // The exact projection ArchivedTimelineLoaderV2 requests when it only needs instant times
        Schema projectedAvroSchema = LSMTimeline.getReadSchema(HoodieArchivedTimeline.LoadMode.TIME);
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
        }
    }

    @Test
    public void testFooterMetadataAbsent()
            throws Exception
    {
        // A timeline file is not a data file: it carries neither a bloom filter nor min/max record keys
        try (TrinoParquetFileReader reader = createReader()) {
            assertThat(reader.readBloomFilter()).isNull();
            assertThat(reader.readMinMaxRecordKeys()).isEmpty();
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
