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
import io.trino.metastore.HiveType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hudi.testing.TpchHudiTablesInitializer.FIELD_UUID;
import static org.apache.hudi.common.model.HoodieRecord.HOODIE_META_COLUMNS;
import static org.apache.hudi.hive.util.HiveSchemaUtil.hoodieSchemaToMapSchema;

public final class HudiTestUtils
{
    private HudiTestUtils() {}

    public static final String COLUMNS_TO_HIDE = String.join(",", ImmutableList.<String>builder()
            .addAll(HOODIE_META_COLUMNS)
            .add(FIELD_UUID)
            .build());

    /**
     * The schema Hive sync reads from a table to build its metastore columns: the latest table schema, with the
     * {@code _hoodie_*} meta fields unless the sync omits them ({@code hoodie.datasource.hive_sync.omit_metadata_fields}).
     */
    public static HoodieSchema hiveSyncStorageSchema(HoodieTableMetaClient metaClient, boolean omitMetadataFields)
    {
        try {
            return new TableSchemaResolver(metaClient).getTableSchema(!omitMetadataFields);
        }
        catch (Exception e) {
            throw new RuntimeException("Could not resolve the table schema of " + metaClient.getBasePath(), e);
        }
    }

    /**
     * The data columns Hive sync registers in the metastore for a table with {@code storageSchema} (see
     * {@link #hiveSyncStorageSchema}), the way its HMS path does: one column per schema field, in schema order, typed by
     * {@code HiveSchemaUtil}, minus the partition keys, which Hive sync registers as partition columns instead. Names
     * are lowercased as the metastore stores them.
     */
    public static List<Column> metastoreDataColumns(HoodieSchema storageSchema, boolean supportTimestamp, List<String> partitionKeys)
    {
        Set<String> partitionNames = partitionKeys.stream()
                .map(name -> name.toLowerCase(Locale.ROOT))
                .collect(toImmutableSet());
        return hoodieSchemaToMapSchema(storageSchema, supportTimestamp, false).entrySet().stream()
                .map(field -> new Column(
                        field.getKey().toLowerCase(Locale.ROOT),
                        HiveType.valueOf(field.getValue().toLowerCase(Locale.ROOT)),
                        Optional.empty(),
                        Map.of()))
                .filter(column -> !partitionNames.contains(column.getName()))
                .collect(toImmutableList());
    }

    /** {@link #metastoreDataColumns(HoodieSchema, boolean, List)} for a non-partitioned table. */
    public static List<Column> metastoreDataColumns(HoodieSchema storageSchema, boolean supportTimestamp)
    {
        return metastoreDataColumns(storageSchema, supportTimestamp, ImmutableList.of());
    }
}
