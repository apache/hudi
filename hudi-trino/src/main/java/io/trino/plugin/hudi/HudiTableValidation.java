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
import com.google.common.collect.ImmutableSet;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import org.apache.hudi.common.schema.HoodieSchemaUtils;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hudi.HudiTableProperties.ORDERING_FIELDS_PROPERTY;
import static io.trino.plugin.hudi.HudiTableProperties.PARTITIONED_BY_PROPERTY;
import static io.trino.plugin.hudi.HudiTableProperties.PRIMARY_KEY_PROPERTY;
import static io.trino.plugin.hudi.HudiTableProperties.getOrderingFields;
import static io.trino.plugin.hudi.HudiTableProperties.getPartitionedBy;
import static io.trino.plugin.hudi.HudiTableProperties.getPrimaryKey;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static java.lang.String.format;

/**
 * Checks that a {@code CREATE TABLE} statement's properties cohere with its column list.
 * <p>
 * Trino has already type-checked each property individually through {@code PropertyMetadata} by the
 * time this runs -- {@code table_type} is an enum and cannot hold an unsupported value, for
 * instance. What is left is whether the properties agree with each other and with the columns, which
 * nothing but the connector can know.
 * <p>
 * All of it runs before any storage is touched, so a rejected statement leaves nothing behind.
 */
public final class HudiTableValidation
{
    private HudiTableValidation() {}

    public static void validateCreateTable(ConnectorTableMetadata tableMetadata)
    {
        List<ColumnMetadata> columns = tableMetadata.getColumns();
        if (columns.isEmpty()) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "Cannot create a table with no columns");
        }

        List<String> columnNames = columns.stream()
                .map(ColumnMetadata::getName)
                .collect(toImmutableList());
        Set<String> uniqueColumnNames = new LinkedHashSet<>();
        for (String columnName : columnNames) {
            if (!uniqueColumnNames.add(columnName)) {
                throw new TrinoException(INVALID_TABLE_PROPERTY, "Duplicate column name: " + columnName);
            }
            if (HoodieSchemaUtils.isMetadataField(columnName)) {
                // HoodieSchemaUtils#addMetadataFields drops an existing meta field rather than
                // duplicating it, so this column would vanish and be replaced by Hudi's own
                // definition of it -- a different type, silently.
                throw new TrinoException(INVALID_TABLE_PROPERTY, format(
                        "Column name '%s' is reserved: Hudi adds its own meta fields to every table", columnName));
            }
        }

        Map<String, Object> properties = tableMetadata.getProperties();
        validatePartitionColumns(columnNames, getPartitionedBy(properties));
        // getOrderingFields also rejects setting both ordering_fields and the deprecated
        // precombine_field alias, since the two write the same Hudi config.
        validateColumnsExist(uniqueColumnNames, getOrderingFields(properties), ORDERING_FIELDS_PROPERTY);
        validateColumnsExist(uniqueColumnNames, getPrimaryKey(properties), PRIMARY_KEY_PROPERTY);
    }

    private static void validatePartitionColumns(List<String> columnNames, List<String> partitionedBy)
    {
        if (partitionedBy.isEmpty()) {
            return;
        }
        validateColumnsExist(ImmutableSet.copyOf(columnNames), partitionedBy, PARTITIONED_BY_PROPERTY);

        Set<String> unique = new LinkedHashSet<>();
        for (String partitionColumn : partitionedBy) {
            if (!unique.add(partitionColumn)) {
                throw new TrinoException(INVALID_TABLE_PROPERTY, format(
                        "Partition column '%s' is listed more than once in %s", partitionColumn, PARTITIONED_BY_PROPERTY));
            }
        }

        if (columnNames.size() == partitionedBy.size()) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "Table contains only partition columns");
        }

        // The same constraint the Hive connector enforces, for the same reason: the metastore holds
        // partition columns in their own list, and a reader reassembles a table as data columns
        // followed by partition columns. Declaring them anywhere but last means the table reads back
        // with its columns in a different order than they were written down in.
        List<String> trailing = ImmutableList.copyOf(
                columnNames.subList(columnNames.size() - partitionedBy.size(), columnNames.size()));
        if (!trailing.equals(partitionedBy)) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, format(
                    "Partition columns must be the last columns in the table and in the same order as %s: expected %s but the table ends with %s",
                    PARTITIONED_BY_PROPERTY, partitionedBy, trailing));
        }
    }

    private static void validateColumnsExist(Set<String> columnNames, List<String> referenced, String propertyName)
    {
        for (String name : referenced) {
            if (!columnNames.contains(name)) {
                throw new TrinoException(INVALID_TABLE_PROPERTY, format(
                        "Column '%s' in %s is not present in the table schema. Columns: %s",
                        name, propertyName, String.join(", ", columnNames)));
            }
        }
    }
}
