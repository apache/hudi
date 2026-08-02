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

import io.trino.metastore.HiveType;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveColumnProjectionInfo;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.hudi.HudiPageSourceProvider.remapColumnIndicesToPhysical;
import static io.trino.plugin.hudi.HudiPageSourceProvider.remapPredicateColumnIndicesToPhysical;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.assertj.core.api.Assertions.assertThat;

class TestHudiPageSourceProviderTest
{
    @Test
    public void testRemapSimpleMatchCaseInsensitive()
    {
        // Physical Schema: [col_a (int), col_b (string)]
        MessageType fileSchema = new MessageType("file_schema",
                Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, OPTIONAL).named("col_a"),
                Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, OPTIONAL).as(LogicalTypeAnnotation.stringType()).named("col_b"));

        // Requested Columns (same order, different case)
        List<HiveColumnHandle> requestedColumns = List.of(
                createDummyHandle("COL_A", 0, HiveType.HIVE_INT, INTEGER),
                createDummyHandle("COL_B", 1, HiveType.HIVE_STRING, VARCHAR));

        // Perform remapping (case-insensitive)
        List<HiveColumnHandle> remapped = remapColumnIndicesToPhysical(fileSchema, requestedColumns, false);

        assertThat(remapped).hasSize(2);
        // First requested column "COL_A" should map to physical index 0
        assertHandle(remapped.get(0), "COL_A", 0, HiveType.HIVE_INT, INTEGER);
        // Second requested column "COL_B" should map to physical index 1
        assertHandle(remapped.get(1), "COL_B", 1, HiveType.HIVE_STRING, VARCHAR);
    }

    @Test
    public void testRemapSimpleMatchCaseSensitive()
    {
        // Physical Schema: [col_a (int), Col_B (string)] - Note the case difference
        MessageType fileSchema = new MessageType("file_schema",
                Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, OPTIONAL).named("col_a"),
                Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, OPTIONAL).as(LogicalTypeAnnotation.stringType()).named("Col_B"));

        // Requested Columns (matching case)
        List<HiveColumnHandle> requestedColumns = List.of(
                createDummyHandle("col_a", 0, HiveType.HIVE_INT, INTEGER),
                createDummyHandle("Col_B", 1, HiveType.HIVE_STRING, VARCHAR));

        // Perform remapping (case-sensitive)
        List<HiveColumnHandle> remapped = remapColumnIndicesToPhysical(fileSchema, requestedColumns, true);

        assertThat(remapped).hasSize(2);
        assertHandle(remapped.get(0), "col_a", 0, HiveType.HIVE_INT, INTEGER);
        assertHandle(remapped.get(1), "Col_B", 1, HiveType.HIVE_STRING, VARCHAR);
    }

    @Test
    public void testRemapCaseSensitiveMismatch()
    {
        // Physical Schema: [col_a (int), col_b (string)]
        MessageType fileSchema = new MessageType("file_schema",
                Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, OPTIONAL).named("col_a"),
                Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, OPTIONAL).as(LogicalTypeAnnotation.stringType()).named("col_b"));

        // Requested Columns (different case)
        List<HiveColumnHandle> requestedColumns = List.of(
                createDummyHandle("COL_A", 0, HiveType.HIVE_INT, INTEGER), // This will mismatch
                createDummyHandle("col_b", 1, HiveType.HIVE_STRING, VARCHAR));

        // Perform remapping (case-sensitive) - "COL_A" won't be found
        List<HiveColumnHandle> remapped = remapColumnIndicesToPhysical(fileSchema, requestedColumns, true);

        assertThat(remapped).hasSize(2);
        // An unmatched column maps one past the last physical field, so the parquet reader null-fills it
        assertHandle(remapped.get(0), "COL_A", fileSchema.getFieldCount(), HiveType.HIVE_INT, INTEGER);
        assertHandle(remapped.get(1), "col_b", 1, HiveType.HIVE_STRING, VARCHAR);
    }

    @Test
    public void testRemapDifferentOrder()
    {
        // Physical Schema: [id (int), name (string), timestamp (long)]
        MessageType fileSchema = new MessageType("file_schema",
                Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, OPTIONAL).named("id"),
                Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, OPTIONAL).as(LogicalTypeAnnotation.stringType()).named("name"),
                Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, OPTIONAL).named("timestamp"));

        // Requested Columns (different order)
        List<HiveColumnHandle> requestedColumns = List.of(
                // Original index irrelevant
                createDummyHandle("name", 99, HiveType.HIVE_STRING, VARCHAR),
                createDummyHandle("timestamp", 5, HiveType.HIVE_LONG, BigintType.BIGINT),
                createDummyHandle("id", 0, HiveType.HIVE_INT, INTEGER));

        // Perform remapping (case-insensitive)
        List<HiveColumnHandle> remapped = remapColumnIndicesToPhysical(fileSchema, requestedColumns, false);

        assertThat(remapped).hasSize(3);
        // First requested "name" -> physical index 1
        assertHandle(remapped.get(0), "name", 1, HiveType.HIVE_STRING, VARCHAR);
        // Second requested "timestamp" -> physical index 2
        assertHandle(remapped.get(1), "timestamp", 2, HiveType.HIVE_LONG, BigintType.BIGINT);
        // Third requested "id" -> physical index 0
        assertHandle(remapped.get(2), "id", 0, HiveType.HIVE_INT, INTEGER);
    }

    @Test
    public void testRemapSubset()
    {
        // Physical Schema: [col_a, col_b, col_c, col_d]
        MessageType fileSchema = new MessageType("file_schema",
                Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, OPTIONAL).named("col_a"),
                Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, OPTIONAL).as(LogicalTypeAnnotation.stringType()).named("col_b"),
                Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, OPTIONAL).named("col_c"),
                Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, OPTIONAL).named("col_d"));

        // Requested Columns (subset and different order)
        List<HiveColumnHandle> requestedColumns = List.of(
                createDummyHandle("col_d", 1, HiveType.HIVE_DOUBLE, DOUBLE),
                createDummyHandle("col_a", 0, HiveType.HIVE_INT, INTEGER));

        // Perform remapping (case-insensitive)
        List<HiveColumnHandle> remapped = remapColumnIndicesToPhysical(fileSchema, requestedColumns, false);

        assertThat(remapped).hasSize(2);
        // First requested "col_d" -> physical index 3
        assertHandle(remapped.get(0), "col_d", 3, HiveType.HIVE_DOUBLE, DOUBLE);
        // Second requested "col_a" -> physical index 0
        assertHandle(remapped.get(1), "col_a", 0, HiveType.HIVE_INT, INTEGER);
    }

    @Test
    public void testRemapEmptyRequested()
    {
        // Physical Schema: [col_a, col_b]
        MessageType fileSchema = new MessageType("file_schema",
                Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, OPTIONAL).named("col_a"),
                Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, OPTIONAL).as(LogicalTypeAnnotation.stringType()).named("col_b"));

        // Requested Columns (empty list)
        List<HiveColumnHandle> requestedColumns = List.of();

        // Perform remapping
        List<HiveColumnHandle> remapped = remapColumnIndicesToPhysical(fileSchema, requestedColumns, false);

        assertThat(remapped).isEmpty();
    }

    @Test
    public void testRemapColumnNotFound()
    {
        // Physical Schema: [col_a]
        MessageType fileSchema = new MessageType("file_schema",
                Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, OPTIONAL).named("col_a"));

        // Requested Columns (includes a non-existent column)
        List<HiveColumnHandle> requestedColumns = List.of(
                createDummyHandle("col_a", 0, HiveType.HIVE_INT, INTEGER),
                // Not in schema, e.g. a base file written before the column was added
                createDummyHandle("col_x", 1, HiveType.HIVE_STRING, VARCHAR));

        // Perform remapping (case-insensitive) - "col_x" won't be found
        List<HiveColumnHandle> remapped = remapColumnIndicesToPhysical(fileSchema, requestedColumns, false);

        assertThat(remapped).hasSize(2);
        assertHandle(remapped.get(0), "col_a", 0, HiveType.HIVE_INT, INTEGER);
        // Out of range on purpose: ParquetPageSourceFactory reports such a column as absent and null-fills it
        assertHandle(remapped.get(1), "col_x", fileSchema.getFieldCount(), HiveType.HIVE_STRING, VARCHAR);
    }

    @Test
    public void testRemapPredicateStaleMetastoreOrdinals()
    {
        // Physical Schema: the five Hudi meta columns, then [c0, c1, c2]
        MessageType fileSchema = hudiFileSchema(3);

        // A metastore synced with omit_metadata_fields=true carries no meta columns, so "c2" is numbered 2
        // while it physically sits at 7, and "c0" is numbered 0 while it physically sits at 5.
        HiveColumnHandle staleC2 = createDummyHandle("c2", 2, HiveType.HIVE_INT, INTEGER);
        HiveColumnHandle staleC0 = createDummyHandle("c0", 0, HiveType.HIVE_INT, INTEGER);
        Domain c2Domain = Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, 900L)), false);
        Domain c0Domain = Domain.singleValue(INTEGER, 7L);

        TupleDomain<HiveColumnHandle> remapped = remapPredicateColumnIndicesToPhysical(
                fileSchema,
                TupleDomain.withColumnDomains(Map.of(staleC2, c2Domain, staleC0, c0Domain)),
                false);

        Map<HiveColumnHandle, Domain> domains = remapped.getDomains().orElseThrow();
        assertThat(domains).hasSize(2);
        // Each domain now keys off the column's physical position, so it is matched against that column's statistics
        assertThat(handleOf(domains, "c2").getBaseHiveColumnIndex()).isEqualTo(7);
        assertThat(domains.get(handleOf(domains, "c2"))).isEqualTo(c2Domain);
        assertThat(handleOf(domains, "c0").getBaseHiveColumnIndex()).isEqualTo(5);
        assertThat(domains.get(handleOf(domains, "c0"))).isEqualTo(c0Domain);
    }

    @Test
    public void testRemapPredicateDropsColumnAbsentFromFile()
    {
        // Physical Schema: the five Hudi meta columns, then [c0]
        MessageType fileSchema = hudiFileSchema(1);

        HiveColumnHandle present = createDummyHandle("c0", 0, HiveType.HIVE_INT, INTEGER);
        // Added after this base file was written, so the file does not carry it
        HiveColumnHandle absent = createDummyHandle("c1", 1, HiveType.HIVE_INT, INTEGER);
        Domain presentDomain = Domain.singleValue(INTEGER, 1L);

        TupleDomain<HiveColumnHandle> remapped = remapPredicateColumnIndicesToPhysical(
                fileSchema,
                TupleDomain.withColumnDomains(Map.of(present, presentDomain, absent, Domain.singleValue(INTEGER, 2L))),
                false);

        // The absent column is dropped rather than mapped to the projection remap's out-of-range sentinel
        Map<HiveColumnHandle, Domain> domains = remapped.getDomains().orElseThrow();
        assertThat(domains).hasSize(1);
        assertThat(handleOf(domains, "c0").getBaseHiveColumnIndex()).isEqualTo(5);
        assertThat(domains.get(handleOf(domains, "c0"))).isEqualTo(presentDomain);
    }

    @Test
    public void testRemapPredicateWithSeveralAbsentColumnsDoesNotCollide()
    {
        // Physical Schema: the five Hudi meta columns, then [c0]
        MessageType fileSchema = hudiFileSchema(1);

        HiveColumnHandle firstAbsent = createDummyHandle("c1", 1, HiveType.HIVE_INT, INTEGER);
        HiveColumnHandle secondAbsent = createDummyHandle("c2", 2, HiveType.HIVE_INT, INTEGER);

        TupleDomain<HiveColumnHandle> remapped = remapPredicateColumnIndicesToPhysical(
                fileSchema,
                TupleDomain.withColumnDomains(Map.of(
                        firstAbsent, Domain.singleValue(INTEGER, 1L),
                        secondAbsent, Domain.singleValue(INTEGER, 2L))),
                false);

        // Both would share the sentinel index, which TupleDomain.transformKeys rejects as a duplicate key.
        // Dropping them instead leaves nothing to push down, and the engine still applies the filter itself.
        assertThat(remapped.isAll()).isTrue();
    }

    @Test
    public void testRemapPredicateKeepsOneDomainPerPhysicalColumn()
    {
        // Physical Schema: the five Hudi meta columns, then [c0]
        MessageType fileSchema = hudiFileSchema(1);

        // Two handles whose names differ only by case resolve to the same file field, so both land on physical
        // index 5 while remaining unequal to each other. The connector cannot produce this - Hive normalises
        // column names to lower case - but pushing both down would hand getParquetTupleDomain one
        // ColumnDescriptor twice, which it rejects by failing the whole split.
        HiveColumnHandle upperCase = createDummyHandle("C0", 0, HiveType.HIVE_INT, INTEGER);
        HiveColumnHandle lowerCase = createDummyHandle("c0", 3, HiveType.HIVE_INT, INTEGER);
        Domain firstDomain = Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, 10L)), false);
        // Insertion-ordered so that "first wins" is a deterministic assertion
        Map<HiveColumnHandle, Domain> predicate = new LinkedHashMap<>();
        predicate.put(upperCase, firstDomain);
        predicate.put(lowerCase, Domain.create(ValueSet.ofRanges(Range.lessThan(INTEGER, 20L)), false));

        TupleDomain<HiveColumnHandle> remapped = remapPredicateColumnIndicesToPhysical(
                fileSchema, TupleDomain.withColumnDomains(predicate), false);

        // Only the first is pushed down, and no IllegalArgumentException escapes
        Map<HiveColumnHandle, Domain> domains = remapped.getDomains().orElseThrow();
        assertThat(domains).hasSize(1);
        HiveColumnHandle survivor = handleOf(domains, "C0");
        assertThat(survivor.getBaseHiveColumnIndex()).isEqualTo(5);
        assertThat(domains.get(survivor)).isEqualTo(firstDomain);
    }

    @Test
    public void testRemapPreservesTheBaseTypeOfADereferenceHandle()
    {
        // Physical Schema: the five Hudi meta columns, then [c0]
        MessageType fileSchema = hudiFileSchema(1);

        // A handle projecting one field out of a struct column. HudiMetadata does not implement applyProjection,
        // so the connector never builds one today, but the remap has to rebuild it without corrupting it: the
        // constructor's type argument is the BASE column's type, while getType() is the projected field's.
        RowType baseType = RowType.rowType(RowType.field("f", INTEGER));
        HiveColumnHandle dereference = new HiveColumnHandle(
                "c0",
                0,
                HiveType.valueOf("struct<f:int>"),
                baseType,
                Optional.of(new HiveColumnProjectionInfo(List.of(0), List.of("f"), HiveType.HIVE_INT, INTEGER)),
                HiveColumnHandle.ColumnType.REGULAR,
                Optional.empty());

        HiveColumnHandle remapped = remapColumnIndicesToPhysical(fileSchema, List.of(dereference), false).get(0);

        assertThat(remapped.getBaseHiveColumnIndex())
                .as("physical index")
                .isEqualTo(5);
        assertThat(remapped.getBaseType())
                .as("base type, which is what the parquet page source reads")
                .isEqualTo(baseType);
        assertThat(remapped.getType())
                .as("projected field type")
                .isEqualTo(INTEGER);
        assertThat(remapped.getHiveColumnProjectionInfo())
                .as("projection info")
                .isEqualTo(dereference.getHiveColumnProjectionInfo());
    }

    @Test
    public void testRemapPredicateAllAndNonePassThrough()
    {
        MessageType fileSchema = hudiFileSchema(1);

        assertThat(remapPredicateColumnIndicesToPhysical(fileSchema, TupleDomain.<HiveColumnHandle>all(), false))
                .isEqualTo(TupleDomain.all());
        assertThat(remapPredicateColumnIndicesToPhysical(fileSchema, TupleDomain.<HiveColumnHandle>none(), false))
                .isEqualTo(TupleDomain.none());
    }

    @Test
    public void testRemapPredicateCaseSensitivity()
    {
        // Physical Schema: the five Hudi meta columns, then [c0]
        MessageType fileSchema = hudiFileSchema(1);

        HiveColumnHandle upperCase = createDummyHandle("C0", 0, HiveType.HIVE_INT, INTEGER);
        TupleDomain<HiveColumnHandle> predicate = TupleDomain.withColumnDomains(Map.of(upperCase, Domain.singleValue(INTEGER, 1L)));

        // Case-insensitive: "C0" resolves to the file's "c0" at physical index 5
        Map<HiveColumnHandle, Domain> insensitive = remapPredicateColumnIndicesToPhysical(fileSchema, predicate, false)
                .getDomains().orElseThrow();
        assertThat(handleOf(insensitive, "C0").getBaseHiveColumnIndex()).isEqualTo(5);

        // Case-sensitive: no match, so the domain is dropped instead of being left on a stale ordinal
        assertThat(remapPredicateColumnIndicesToPhysical(fileSchema, predicate, true).isAll()).isTrue();
    }

    @Test
    public void testRemapPredicatePreservesEveryOtherHandleAttribute()
    {
        // Physical Schema: the five Hudi meta columns, then [c0]
        MessageType fileSchema = hudiFileSchema(1);

        HiveColumnHandle original = new HiveColumnHandle(
                "c0",
                0,
                HiveType.HIVE_INT,
                INTEGER,
                Optional.empty(),
                HiveColumnHandle.ColumnType.REGULAR,
                Optional.of("a comment"));
        Domain domain = Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, 900L)), true);

        Map<HiveColumnHandle, Domain> domains = remapPredicateColumnIndicesToPhysical(
                fileSchema,
                TupleDomain.withColumnDomains(Map.of(original, domain)),
                false)
                .getDomains().orElseThrow();

        HiveColumnHandle remapped = handleOf(domains, "c0");
        assertHandle(remapped, "c0", 5, HiveType.HIVE_INT, INTEGER);
        assertThat(remapped.getComment())
                .as("Comment mismatch for c0")
                .isEqualTo(Optional.of("a comment"));
        assertThat(domains.get(remapped))
                .as("Domain mismatch for c0")
                .isEqualTo(domain);
    }

    /**
     * Builds a file schema laid out like a Hudi base file: the five {@code _hoodie_*} meta columns followed by
     * {@code dataColumnCount} int columns named {@code c0..cN}. A metastore synced with
     * {@code hoodie.datasource.hive_sync.omit_metadata_fields=true} omits the meta columns, so a data column's
     * metastore ordinal is its physical ordinal minus five.
     */
    private static MessageType hudiFileSchema(int dataColumnCount)
    {
        List<org.apache.parquet.schema.Type> fields = new ArrayList<>();
        for (String metaColumn : List.of("_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key", "_hoodie_partition_path", "_hoodie_file_name")) {
            fields.add(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, OPTIONAL).as(LogicalTypeAnnotation.stringType()).named(metaColumn));
        }
        for (int i = 0; i < dataColumnCount; i++) {
            fields.add(Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, OPTIONAL).named("c" + i));
        }
        return new MessageType("file_schema", fields);
    }

    /**
     * Returns the single remapped handle carrying the given base column name.
     */
    private static HiveColumnHandle handleOf(Map<HiveColumnHandle, Domain> domains, String baseColumnName)
    {
        return domains.keySet().stream()
                .filter(handle -> handle.getBaseColumnName().equals(baseColumnName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No domain was kept for column " + baseColumnName));
    }

    /**
     * Creates a basic HiveColumnHandle for testing.
     * Assumes REGULAR column type and no projection info or comments.
     * The initial hiveColumnIndex is often irrelevant for this specific test, as we are testing the remapping logic.
     *
     * @param name Name of the column handle
     * @param initialIndex The original index before remapping which might not be the physical one
     * @param hiveType Hive type of column handle
     * @param trinoType Trino type of column handle
     */
    private HiveColumnHandle createDummyHandle(
            String name,
            int initialIndex,
            HiveType hiveType,
            Type trinoType)
    {
        return new HiveColumnHandle(
                name,
                initialIndex,
                hiveType,
                trinoType,
                Optional.empty(),
                HiveColumnHandle.ColumnType.REGULAR,
                Optional.empty());
    }

    /**
     * Asserts that a HiveColumnHandle has the expected properties after remapping.
     */
    private void assertHandle(
            HiveColumnHandle handle,
            String expectedBaseName,
            int expectedPhysicalIndex,
            HiveType expectedHiveType,
            Type expectedTrinoType)
    {
        assertThat(handle.getBaseColumnName())
                .as("BaseColumnName mismatch for %s", expectedBaseName)
                .isEqualTo(expectedBaseName);
        assertThat(handle.getBaseHiveColumnIndex())
                .as("BaseHiveColumnIndex (physical) mismatch for %s", expectedBaseName)
                .isEqualTo(expectedPhysicalIndex);
        assertThat(handle.getBaseHiveType())
                .as("BaseHiveType mismatch for %s", expectedBaseName)
                .isEqualTo(expectedHiveType);
        assertThat(handle.getType())
                .as("Trino Type mismatch for %s", expectedBaseName)
                .isEqualTo(expectedTrinoType);
        // Assert that other fields if they are relevant
        assertThat(handle.getColumnType())
                .as("ColumnType mismatch for %s", expectedBaseName)
                .isEqualTo(HiveColumnHandle.ColumnType.REGULAR);
    }
}
