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
import io.trino.plugin.hudi.testing.MaxRankRecordMerger;
import org.apache.avro.SchemaBuilder;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hudi.HudiUtil.appendMissingMergeRequiredColumns;
import static io.trino.plugin.hudi.HudiUtil.mergeRequiredColumnNames;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.apache.hudi.common.config.HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY;
import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_KEY;
import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_MARKER;
import static org.apache.hudi.common.model.HoodieRecord.HOODIE_IS_DELETED_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.OPERATION_METADATA_FIELD;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link HudiUtil#mergeRequiredColumnNames}, which mirrors the non-CUSTOM branch of the file-group
 * reader's {@code FileGroupReaderSchemaHandler.getMandatoryFieldsForMerging} so the base-file read
 * projection carries every column the merge may consult (ordering fields, delete markers, the operation
 * field, record-key data columns) even when the query does not project them, plus the merge-path recovery
 * of names the metastore could not resolve ({@link HudiUtil#appendMissingMergeRequiredColumns}).
 */
class TestHudiMergeRequiredColumns
{
    @Test
    public void testOrderingFieldsFollowMergeMode()
    {
        HoodieTableConfig tableConfig = new HoodieTableConfig();
        tableConfig.setValue(HoodieTableConfig.ORDERING_FIELDS, "ts,seq");

        assertThat(mergeRequiredColumnNames(tableConfig, RecordMergeMode.EVENT_TIME_ORDERING))
                .contains("ts", "seq");
        // Commit-time merging ignores ordering fields
        assertThat(mergeRequiredColumnNames(tableConfig, RecordMergeMode.COMMIT_TIME_ORDERING))
                .doesNotContain("ts", "seq");
        assertThat(mergeRequiredColumnNames(tableConfig, null))
                .doesNotContain("ts", "seq");
    }

    @Test
    public void testDeleteAndOperationColumnsAlwaysRequested()
    {
        // Requested unconditionally: getMergeRequiredColumnHandles keeps only metastore data columns, and
        // the merge path's appendMissingMergeRequiredColumns recovers schema-carried names, so a table
        // with these fields in neither reads nothing extra
        assertThat(mergeRequiredColumnNames(new HoodieTableConfig(), RecordMergeMode.COMMIT_TIME_ORDERING))
                .contains(HOODIE_IS_DELETED_FIELD, OPERATION_METADATA_FIELD);
    }

    @Test
    public void testCustomDeleteKeyRequiresMarkerToo()
    {
        HoodieTableConfig withKeyAndMarker = new HoodieTableConfig();
        withKeyAndMarker.setValue(DELETE_KEY, "op");
        withKeyAndMarker.setValue(DELETE_MARKER, "D");
        assertThat(mergeRequiredColumnNames(withKeyAndMarker, RecordMergeMode.EVENT_TIME_ORDERING))
                .contains("op");

        // DeleteContext only honors the delete key when the marker value is also set
        HoodieTableConfig keyOnly = new HoodieTableConfig();
        keyOnly.setValue(DELETE_KEY, "op");
        assertThat(mergeRequiredColumnNames(keyOnly, RecordMergeMode.EVENT_TIME_ORDERING))
                .doesNotContain("op");
    }

    @Test
    public void testRecordKeyFieldsRequestedWithoutPopulatedMetaFields()
    {
        HoodieTableConfig tableConfig = new HoodieTableConfig();
        tableConfig.setValue(HoodieTableConfig.RECORDKEY_FIELDS, "id1,id2");

        // With populated meta fields (the default) the merge keys on _hoodie_record_key, which the
        // connector always prepends into the projection
        assertThat(mergeRequiredColumnNames(tableConfig, RecordMergeMode.EVENT_TIME_ORDERING))
                .doesNotContain("id1", "id2");

        tableConfig.setValue(HoodieTableConfig.POPULATE_META_FIELDS, "false");
        assertThat(mergeRequiredColumnNames(tableConfig, RecordMergeMode.EVENT_TIME_ORDERING))
                .contains("id1", "id2");
    }

    @Test
    public void testMergeRequiredColumnMissingFromProjectionResolvesFromTableSchema()
    {
        // The metastore never resolved _hoodie_operation (hive sync with omit_metadata_fields=true), so the
        // projection arrives on the merge path without it; it must be recovered from the resolved table
        // schema -- the gate the file-group reader itself applies -- along with the ordering field, while
        // _hoodie_is_deleted, in neither the projection nor the schema, stays dropped
        HoodieSchema dataSchema = HoodieSchema.fromAvroSchema(SchemaBuilder.record("rec").fields()
                .requiredString("id")
                .requiredLong("ts")
                .requiredString(OPERATION_METADATA_FIELD)
                .endRecord());
        List<HiveColumnHandle> projection = List.of(
                createBaseColumn("id", 0, HiveType.HIVE_STRING, VARCHAR, REGULAR, Optional.empty()));

        List<HiveColumnHandle> extended = appendMissingMergeRequiredColumns(dataSchema, projection, eventTimeOrderedOn("ts"), new TypedProperties());

        assertThat(extended).extracting(HiveColumnHandle::getName)
                .containsExactly("id", "ts", OPERATION_METADATA_FIELD);
        // The recovered handles are typed from their Avro fields
        assertThat(extended.get(1).getType()).isEqualTo(BIGINT);
        assertThat(extended.getLast().getType()).isEqualTo(VARCHAR);
    }

    @Test
    public void testCustomMergerMandatoryFieldMissingFromProjectionResolvesFromTableSchema()
    {
        // MaxRankRecordMerger declares merge_rank mandatory; a metastore that does not carry the column
        // leaves it out of the projection, and the merge path must recover it by asking the same resolved
        // merger the file-group reader will use
        assertThat(appendMissingMergeRequiredColumns(
                rankTableSchema(), idOnlyProjection(), customMergeOrderedOn("ts", MaxRankRecordMerger.MERGE_STRATEGY_ID), maxRankMergerProps()))
                .extracting(HiveColumnHandle::getName)
                .containsExactly("id", "ts", MaxRankRecordMerger.RANK_COLUMN);
    }

    @Test
    public void testCustomMergeModeWithoutStrategyIdSkipsMergerResolution()
    {
        // A CUSTOM table without a persisted strategy id cannot resolve a merger; the append must fall
        // back to the table-config-derived names without dereferencing the null id -- the read then fails
        // actionably in getRecordMerger, not with an NPE here
        assertThat(appendMissingMergeRequiredColumns(
                rankTableSchema(), idOnlyProjection(), customMergeOrderedOn("ts", null), maxRankMergerProps()))
                .extracting(HiveColumnHandle::getName)
                .containsExactly("id", "ts");
    }

    @Test
    public void testCustomMergeModeWithUnresolvableMergerSkipsMandatoryFields()
    {
        // A strategy id no configured merger implementation declares resolves nothing; the append keeps
        // the table-config-derived names and must not dereference the empty resolution. (The all-zeros
        // uuid would NOT do here: it is PAYLOAD_BASED_MERGE_STRATEGY_UUID, which always resolves.)
        assertThat(appendMissingMergeRequiredColumns(
                rankTableSchema(), idOnlyProjection(), customMergeOrderedOn("ts", "e2a5b7c9-1d3f-4a68-9c0b-5e7d9f1a3b6c"), maxRankMergerProps()))
                .extracting(HiveColumnHandle::getName)
                .containsExactly("id", "ts");
    }

    @Test
    public void testMergeRequiredColumnAlreadyProjectedIsNotDuplicated()
    {
        HoodieSchema dataSchema = HoodieSchema.fromAvroSchema(SchemaBuilder.record("rec").fields()
                .requiredString("id")
                .requiredLong("ts")
                .endRecord());
        // The projection carries the ordering field under a different case; the append must match
        // case-insensitively and leave the projection handle untouched
        List<HiveColumnHandle> projection = List.of(
                createBaseColumn("TS", 1, HiveType.HIVE_LONG, BIGINT, REGULAR, Optional.empty()));

        assertThat(appendMissingMergeRequiredColumns(dataSchema, projection, eventTimeOrderedOn("ts"), new TypedProperties()))
                .extracting(HiveColumnHandle::getName)
                .containsExactly("TS");
    }

    private static HoodieSchema rankTableSchema()
    {
        return HoodieSchema.fromAvroSchema(SchemaBuilder.record("rec").fields()
                .requiredString("id")
                .requiredLong("ts")
                .requiredLong(MaxRankRecordMerger.RANK_COLUMN)
                .endRecord());
    }

    private static List<HiveColumnHandle> idOnlyProjection()
    {
        return List.of(createBaseColumn("id", 0, HiveType.HIVE_STRING, VARCHAR, REGULAR, Optional.empty()));
    }

    private static HoodieTableConfig customMergeOrderedOn(String orderingField, String mergeStrategyId)
    {
        HoodieTableConfig tableConfig = new HoodieTableConfig();
        tableConfig.setValue(HoodieTableConfig.VERSION, "9");
        tableConfig.setValue(HoodieTableConfig.RECORD_MERGE_MODE, RecordMergeMode.CUSTOM.name());
        if (mergeStrategyId != null) {
            tableConfig.setValue(HoodieTableConfig.RECORD_MERGE_STRATEGY_ID, mergeStrategyId);
        }
        tableConfig.setValue(HoodieTableConfig.ORDERING_FIELDS, orderingField);
        return tableConfig;
    }

    private static TypedProperties maxRankMergerProps()
    {
        TypedProperties readerProps = new TypedProperties();
        readerProps.setProperty(RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY, MaxRankRecordMerger.class.getName());
        return readerProps;
    }

    private static HoodieTableConfig eventTimeOrderedOn(String orderingField)
    {
        // Pin the version so the names under test are not at the mercy of the pre-v9 merge-config inference
        HoodieTableConfig tableConfig = new HoodieTableConfig();
        tableConfig.setValue(HoodieTableConfig.VERSION, "9");
        tableConfig.setValue(HoodieTableConfig.RECORD_MERGE_MODE, RecordMergeMode.EVENT_TIME_ORDERING.name());
        tableConfig.setValue(HoodieTableConfig.ORDERING_FIELDS, orderingField);
        return tableConfig;
    }
}
