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
import io.trino.parquet.ParquetCorruptionException;
import io.trino.plugin.hudi.storage.HudiTrinoStorage;
import io.trino.plugin.hudi.storage.TrinoStorageConfiguration;
import io.trino.spi.TrinoException;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.avro.model.HoodieLSMTimelineInstant;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.LSMTimeline;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.parquet.io.ParquetDecodingException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static io.trino.plugin.hudi.HudiErrorCode.HUDI_BAD_DATA;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_CURSOR_ERROR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests {@link TrinoParquetFileReader} against a four-instant LSM archived-timeline parquet file, the shape Hudi's
 * archived-timeline loader reads through the connector. {@code archived_timeline.parquet} is the first history file,
 * {@code 20250918121953134_20250918122001506_0.parquet}, of the table-version-8 COW table that the create script in
 * {@code hudi-testing-data/hudi_cow_archived_timeline.md} produces with Hudi 1.0.2: four commit instants,
 * 20250918121953134 through 20250918122001506. Only that file is checked in, not the table.
 */
class TestTrinoParquetFileReader
{
    private static final String ARCHIVED_TIMELINE_PARQUET_FILE = "archived_timeline.parquet";
    // The fixture's first page is the only data page of instantTime: 25 bytes of thrift-compact PageHeader from
    // byte 4 (type, sizes, crc, then the DataPageHeader whose encoding field is the byte at 22), then the payload.
    // Thrift compact writes the encoding enum as a zigzag varint: PLAIN (0) is 0x00, RLE_DICTIONARY (8) is 0x10
    private static final int INSTANT_TIME_PAGE_ENCODING_OFFSET = 22;
    private static final byte PLAIN_ENCODING = 0x00;
    private static final byte RLE_DICTIONARY_ENCODING = 0x10;

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
    @EnumSource(value = HoodieArchivedTimeline.LoadMode.class, names = {"TIME", "METADATA", "PLAN", "FULL"})
    void testProjectedReadUsesRequestedSchema(HoodieArchivedTimeline.LoadMode loadMode)
            throws Exception
    {
        // The projections the archived-timeline readers request. TIME is what CompletionTimeQueryViewV2 asks for when
        // it only needs instant times; METADATA and PLAN are what ArchivedTimelineV2 asks for when it needs a payload,
        // and it casts that bytes column to ByteBuffer -- the SqlVarbinary -> ByteBuffer conversion exists for those
        // two. FULL is requested only by EightToSevenDowngradeHandler, but it is the one projection that orders plan
        // before metadata, the reverse of the file's metadata, plan, so a passing FULL read proves columns are mapped
        // by name and not by position
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

            // All four instants of the fixture are commits: every row has metadata bytes and a null plan. METADATA and
            // PLAN each carry only their own bytes column and FULL carries both, so the two are checked independently
            if (projectedAvroSchema.getField("metadata") != null) {
                int metadataPos = projectedAvroSchema.getField("metadata").pos();
                assertThat(records).allSatisfy(record -> {
                    assertThat(record.get(metadataPos)).isInstanceOf(ByteBuffer.class);
                    assertThat(((ByteBuffer) record.get(metadataPos)).remaining()).isGreaterThan(0);
                });
            }
            if (projectedAvroSchema.getField("plan") != null) {
                int planPos = projectedAvroSchema.getField("plan").pos();
                assertThat(records).allSatisfy(record -> assertThat(record.get(planPos)).isNull());
            }
        }
    }

    @Test
    void testDrainedIteratorStaysDrained()
            throws Exception
    {
        // Once the ParquetReader has handed out its last page it must not be asked for another: a nextPage() past the
        // end of the row groups throws instead of returning null again, so the iterator has to remember it is done
        HoodieSchema tableSchema = HoodieSchema.fromAvroSchema(HoodieLSMTimelineInstant.getClassSchema());
        try (TrinoParquetFileReader reader = createReader();
                ClosableIterator<IndexedRecord> iterator = reader.getIndexedRecordIterator(tableSchema, tableSchema)) {
            List<IndexedRecord> records = new ArrayList<>();
            iterator.forEachRemaining(records::add);
            assertThat(records).hasSize(4);

            assertThat(iterator.hasNext()).isFalse();
            assertThat(iterator.hasNext()).isFalse();
            assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
        }
    }

    @Test
    void testCloseReleasesOpenIterator()
            throws Exception
    {
        // Closing the reader closes an iterator the caller left open; closing that iterator afterwards is a no-op
        HoodieSchema tableSchema = HoodieSchema.fromAvroSchema(HoodieLSMTimelineInstant.getClassSchema());
        TrinoParquetFileReader reader = createReader();
        ClosableIterator<IndexedRecord> iterator = reader.getIndexedRecordIterator(tableSchema, tableSchema);
        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next()).isNotNull();

        reader.close();
        assertThat(iterator.hasNext()).isFalse();
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
        iterator.close();
        reader.close();
    }

    @Test
    void testCorruptFileFailsWithBadData(@TempDir Path tempDir)
            throws Exception
    {
        // A footer that does not parse is reported the way a failed page read is: HUDI_BAD_DATA with the corruption as
        // its cause. The ParquetCorruptionException it starts out as is an IOException, and left raw it would reach
        // hudi-common as a HoodieIOException instead
        Path corruptFile = tempDir.resolve("corrupt.parquet");
        Files.write(corruptFile, new byte[] {1, 2, 3});
        StoragePath path = new StoragePath(corruptFile.toFile().toURI().toString());

        assertThatThrownBy(() -> new TrinoParquetFileReader(localStorage(), path))
                .isInstanceOf(TrinoException.class)
                .hasCauseInstanceOf(ParquetCorruptionException.class)
                .extracting(e -> ((TrinoException) e).getErrorCode())
                .isEqualTo(HUDI_BAD_DATA.toErrorCode());
    }

    @Test
    void testCorruptDataPageFailsWithCursorError(@TempDir Path tempDir)
            throws Exception
    {
        // A data page that fails to decode is reported the way a failed footer read is: as a TrinoException with the
        // failure as its cause. Trino decodes a page eagerly and reports a corrupt one with a ParquetDecodingException,
        // which is unchecked, so left raw it would pass through hudi-common untouched and reach the engine as an
        // internal error instead of HUDI_CURSOR_ERROR. The footer is intact, so the reader still opens and the failure
        // surfaces from hasNext(). Switching the page's encoding from PLAIN to RLE_DICTIONARY makes it claim a
        // dictionary the column chunk does not carry, which is what the column reader rejects
        byte[] bytes = Resources.toByteArray(Resources.getResource(ARCHIVED_TIMELINE_PARQUET_FILE));
        assertThat(bytes[INSTANT_TIME_PAGE_ENCODING_OFFSET]).isEqualTo(PLAIN_ENCODING);
        bytes[INSTANT_TIME_PAGE_ENCODING_OFFSET] = RLE_DICTIONARY_ENCODING;
        Path corruptFile = tempDir.resolve("corrupt_page.parquet");
        Files.write(corruptFile, bytes);
        StoragePath path = new StoragePath(corruptFile.toFile().toURI().toString());

        HoodieSchema tableSchema = HoodieSchema.fromAvroSchema(HoodieLSMTimelineInstant.getClassSchema());
        try (TrinoParquetFileReader reader = new TrinoParquetFileReader(localStorage(), path);
                ClosableIterator<IndexedRecord> iterator = reader.getIndexedRecordIterator(tableSchema, tableSchema)) {
            assertThatThrownBy(iterator::hasNext)
                    .isInstanceOf(TrinoException.class)
                    .hasCauseInstanceOf(ParquetDecodingException.class)
                    .extracting(e -> ((TrinoException) e).getErrorCode())
                    .isEqualTo(HUDI_CURSOR_ERROR.toErrorCode());
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
        return new TrinoParquetFileReader(localStorage(), new StoragePath(parquetFile.toURI().toString()));
    }

    private static HoodieStorage localStorage()
    {
        return new HudiTrinoStorage(new LocalFileSystem(Paths.get("/")), new TrinoStorageConfiguration());
    }
}
