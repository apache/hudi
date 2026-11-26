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

import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.hudi.storage.TrinoStorageConfiguration;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorTableMetadata;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.storage.StoragePath;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static io.trino.plugin.hudi.HudiErrorCode.HUDI_META_CLIENT_ERROR;
import static io.trino.plugin.hudi.HudiTableProperties.getHiveStylePartitioning;
import static io.trino.plugin.hudi.HudiTableProperties.getHoodieProperties;
import static io.trino.plugin.hudi.HudiTableProperties.getKeyGeneratorClass;
import static io.trino.plugin.hudi.HudiTableProperties.getOrderingFields;
import static io.trino.plugin.hudi.HudiTableProperties.getPartitionedBy;
import static io.trino.plugin.hudi.HudiTableProperties.getPrimaryKey;
import static io.trino.plugin.hudi.HudiTableProperties.getRecordMergeMode;
import static io.trino.plugin.hudi.HudiTableProperties.getTableType;
import static java.lang.String.format;

/**
 * Writes a table's {@code .hoodie} directory. Pure hudi-common: no Spark, no write client and no
 * metastore, so it can run before the catalog entry exists without entering any row-writing path.
 */
public final class HudiTableInitializer
{
    /**
     * The {@code hoodie.table.version} written by this connector.
     * <p>
     * Pinned rather than inherited from {@link HoodieTableVersion#current()} so that a future bump of
     * {@code current()} cannot silently change what the connector emits. Raising this should be a
     * deliberate change with a test diff, not a side effect of upgrading Hudi; the literal assertion
     * in {@code TestHudiTableInitializer} guards this value.
     */
    public static final HoodieTableVersion CREATED_TABLE_VERSION = HoodieTableVersion.TEN;

    private HudiTableInitializer() {}

    /**
     * Initializes storage for an empty table.
     *
     * @param tableSchema the schema that also produces the metastore column list; Hudi metadata
     *         fields are removed only from the persisted table create schema
     */
    public static void initializeTable(
            TrinoFileSystem fileSystem,
            String basePath,
            ConnectorTableMetadata tableMetadata,
            HoodieSchema tableSchema)
    {
        Map<String, Object> properties = tableMetadata.getProperties();
        HoodieTableMetaClient.TableBuilder builder = HoodieTableMetaClient.newTableBuilder()
                .setTableType(getTableType(properties))
                .setTableName(tableMetadata.getTable().getTableName())
                .setDatabaseName(tableMetadata.getTable().getSchemaName())
                .setTableVersion(CREATED_TABLE_VERSION)
                .setTableCreateSchema(HoodieSchemaUtils.removeMetadataFields(tableSchema).toAvroSchema().toString());

        List<String> primaryKey = getPrimaryKey(properties);
        if (!primaryKey.isEmpty()) {
            builder.setRecordKeyFields(String.join(",", primaryKey));
        }
        List<String> partitionedBy = getPartitionedBy(properties);
        if (!partitionedBy.isEmpty()) {
            builder.setPartitionFields(String.join(",", partitionedBy));
        }
        List<String> orderingFields = getOrderingFields(properties);
        if (!orderingFields.isEmpty()) {
            builder.setOrderingFields(String.join(",", orderingFields));
        }
        getRecordMergeMode(properties).ifPresent(builder::setRecordMergeMode);
        getKeyGeneratorClass(properties).ifPresentOrElse(
                builder::setKeyGeneratorClassProp,
                () -> builder.setKeyGeneratorType(inferKeyGeneratorType(primaryKey, partitionedBy).name()));
        builder.setHiveStylePartitioningEnable(getHiveStylePartitioning(properties));

        Map<String, String> passthrough = getHoodieProperties(properties);
        if (!passthrough.isEmpty()) {
            // Applied last so an explicit hoodie.* config wins over anything derived above; that is
            // the escape hatch the passthrough map exists for.
            builder.set(Map.copyOf(passthrough));
        }

        try {
            // Goes through Hudi's pluggable-storage extension point: the configuration names
            // HudiTrinoStorage as HOODIE_STORAGE_CLASS and carries the session's file system, so
            // hudi-common builds storage for the connector itself. No hudi-common change needed.
            builder.initTable(new TrinoStorageConfiguration(fileSystem), new StoragePath(basePath));
        }
        catch (IOException e) {
            throw new TrinoException(HUDI_META_CLIENT_ERROR, format(
                    "Failed to initialize Hudi table metadata for %s at %s",
                    tableMetadata.getTable(), basePath), e);
        }
    }

    /**
     * Mirrors {@code KeyGenUtils.inferKeyGeneratorType} for Trino's plain column-name lists without
     * taking a runtime dependency on hudi-client-common.
     */
    private static KeyGeneratorType inferKeyGeneratorType(List<String> primaryKey, List<String> partitionedBy)
    {
        if (partitionedBy.isEmpty()) {
            return KeyGeneratorType.NON_PARTITION;
        }
        if (partitionedBy.size() == 1 && primaryKey.size() <= 1) {
            return KeyGeneratorType.SIMPLE;
        }
        return KeyGeneratorType.COMPLEX;
    }
}
