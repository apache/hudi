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

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.hudi.HudiUtil.mergeRequiredColumnNames;
import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_KEY;
import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_MARKER;
import static org.apache.hudi.common.model.HoodieRecord.HOODIE_IS_DELETED_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.OPERATION_METADATA_FIELD;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link HudiUtil#mergeRequiredColumnNames}, which mirrors the non-CUSTOM branch of the file-group
 * reader's {@code FileGroupReaderSchemaHandler.getMandatoryFieldsForMerging} so the base-file read
 * projection carries every column the merge may consult (ordering fields, delete markers, the operation
 * field, record-key data columns) even when the query does not project them.
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
        // Requested unconditionally: buildColumnHandles drops names that are not data columns of the
        // table, so tables without these fields read nothing extra
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
}
