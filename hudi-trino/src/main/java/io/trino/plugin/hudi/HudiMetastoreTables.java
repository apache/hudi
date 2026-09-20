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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metastore.Column;
import io.trino.metastore.StorageFormat;
import io.trino.metastore.Table;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.sync.common.util.HoodieMetastoreTableDescriptor;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hive.TableType.MANAGED_TABLE;
import static io.trino.plugin.hudi.HudiUtil.toColumnHandle;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;

/**
 * Translates {@link HoodieMetastoreTableDescriptor} into a Trino {@link Table}.
 * <p>
 * The assembly decisions -- which input format carries the table type, which columns are partition
 * columns, which properties make the table recognisable to Spark SQL, where
 * {@code serialization.format} goes -- are not made here. They live in {@code hudi-sync-common}
 * next to {@code SparkDataSourceTableUtils}, in Hudi and JDK types, so that the normalization contract
 * is reusable by metastore integrations without depending on Trino types. All that happens here is
 * the mapping into {@code io.trino.metastore} types. Parity tests in {@code hudi-hive-sync} pin the
 * shared format names and schema-splitting default to the values its existing executor emits.
 * <p>
 * The columns come from the same {@link HoodieSchema} that becomes
 * {@code hoodie.table.create.schema}, which is the one thing this class does insist on. The
 * connector keeps two independent descriptions of a table's columns -- that schema, and the
 * metastore storage descriptor -- and {@link HudiMetadata#getColumnHandles} reads only the latter.
 * Nothing reconciles them, so a table built from two separately-handled inputs can end up registered
 * with columns that do not match its own schema, or with none at all (HUDI-9435:
 * {@code SELECT * not allowed from relation that has no columns}). Both {@code CREATE TABLE} and
 * {@code register_table} go through here so that cannot happen.
 * <p>
 * Column types come from {@link HudiUtil#toColumnHandle}, the same Avro-to-Trino mapping the read
 * path uses, so the types registered are the types a query will see.
 */
public final class HudiMetastoreTables
{
    private HudiMetastoreTables() {}

    /**
     * Builds the metastore descriptor for a table's snapshot view.
     * <p>
     * Lenient about a partition column that is absent from {@code tableSchema}, typing it as a
     * string, because the shared layer is and because {@code register_table} has to cope with real
     * tables built by key generators that do not record their partition fields in the schema.
     * {@code CREATE TABLE} rejects that case earlier, in {@link HudiTableValidation}, where the
     * offending property can be named.
     *
     * @param tableSchema the table's Hudi schema, including the meta fields; supplies both the
     *         column names and their types
     * @param partitionedBy partition column names, in partition-path order
     * @param external whether the table is external. An explicit {@code location} makes a table
     *         external, an omitted one makes it managed; see {@link HudiMetadata#createTable}.
     */
    public static Table buildTable(
            SchemaTableName tableName,
            String basePath,
            HoodieTableType tableType,
            HoodieSchema tableSchema,
            List<String> partitionedBy,
            boolean external,
            Optional<String> owner,
            Optional<String> comment)
    {
        HoodieMetastoreTableDescriptor descriptor;
        try {
            descriptor = HoodieMetastoreTableDescriptor.forSnapshotView(
                    tableSchema, partitionedBy, tableType, basePath, external);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, e.getMessage(), e);
        }

        ImmutableMap.Builder<String, String> parameters = ImmutableMap.<String, String>builder()
                .putAll(descriptor.getTableParameters());
        comment.ifPresent(value -> parameters.put(Table.TABLE_COMMENT, value));

        return Table.builder()
                .setDatabaseName(tableName.getSchemaName())
                .setTableName(tableName.getTableName())
                // Hive treats the table type and the EXTERNAL parameter as independent; the shared
                // layer supplies the parameter, this supplies the type, and they must agree.
                .setTableType((external ? EXTERNAL_TABLE : MANAGED_TABLE).name())
                .setOwner(owner)
                .setDataColumns(toColumns(descriptor.getDataFields()))
                .setPartitionColumns(toColumns(descriptor.getPartitionFields()))
                .setParameters(parameters.buildKeepingLast())
                .withStorage(storage -> storage
                        .setStorageFormat(StorageFormat.create(
                                descriptor.getSerdeClassName(),
                                descriptor.getInputFormatClassName(),
                                descriptor.getOutputFormatClassName()))
                        .setSerdeParameters(descriptor.getSerdeParameters())
                        .setLocation(basePath))
                .build();
    }

    private static List<Column> toColumns(List<HoodieSchemaField> fields)
    {
        ImmutableList.Builder<Column> columns = ImmutableList.builderWithExpectedSize(fields.size());
        for (HoodieSchemaField field : fields) {
            columns.add(new Column(
                    field.name(),
                    toColumnHandle(field).getHiveType(),
                    field.doc().isPresent() ? Optional.of(field.doc().get()) : Optional.empty(),
                    ImmutableMap.of()));
        }
        return columns.build();
    }
}
