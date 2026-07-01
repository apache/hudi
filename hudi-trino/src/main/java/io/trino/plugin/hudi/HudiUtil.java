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

import com.google.common.cache.Cache;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.metastore.Column;
import io.trino.metastore.HivePartition;
import io.trino.metastore.HiveType;
import io.trino.metastore.Table;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.plugin.hive.avro.AvroHiveFileUtils;
import io.trino.plugin.hudi.storage.HudiTrinoStorage;
import io.trino.plugin.hudi.storage.TrinoStorageConfiguration;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.Lazy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.util.HiveTypeUtil.getType;
import static io.trino.plugin.hive.util.HiveTypeUtil.typeSupported;
import static io.trino.plugin.hive.util.HiveUtil.checkCondition;
import static io.trino.plugin.hive.util.HiveUtil.parsePartitionValue;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMNS;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMN_TYPES;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_BAD_DATA;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_FILESYSTEM_ERROR;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_META_CLIENT_ERROR;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_SCHEMA_ERROR;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_UNSUPPORTED_FILE_FORMAT;
import static java.lang.Math.toIntExact;
import static org.apache.hudi.common.config.HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY;
import static org.apache.hudi.common.model.HoodieRecord.PARTITION_PATH_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_METADATA_FIELD;

public final class HudiUtil
{
    // Minimal meta-column subset the file-group reader/merger requires, distinct from the upstream
    // 5-field HoodieRecord.HOODIE_META_COLUMNS (commit time, seqno, key, partition path, file name).
    public static final List<String> HUDI_REQUIRED_META_COLUMNS =
            CollectionUtils.createImmutableList(RECORD_KEY_METADATA_FIELD, PARTITION_PATH_METADATA_FIELD);

    private static final Logger log = Logger.get(HudiUtil.class);
    private static final Cache<Schema, Map<String, Schema.Field>> SCHEMA_FIELD_CACHE =
            EvictableCacheBuilder.newBuilder()
                    .maximumWeight(10L * 1000L * 1024L) // 10MB
                    .weigher((Weigher<Schema, Map<String, Schema.Field>>) (schema, fieldMap) -> {
                        // approximate size estimation of schema size
                        long schemaSize = estimatedSizeOf(schema.toString());

                        long fieldsSize = fieldMap.entrySet().stream()
                                .mapToLong(e -> estimatedSizeOf(e.getKey()) + estimatedSizeOf(e.getValue().name()))
                                .sum();

                        return toIntExact(schemaSize + fieldsSize);
                    })
                    .expireAfterWrite(5, TimeUnit.MINUTES)
                    .shareNothingWhenDisabled()
                    .build();

    private HudiUtil() {}

    public static HoodieFileFormat getHudiFileFormat(String path)
    {
        String extension = getFileExtension(path);
        if (extension.equals(HoodieFileFormat.PARQUET.getFileExtension())) {
            return HoodieFileFormat.PARQUET;
        }
        if (extension.equals(HoodieFileFormat.HOODIE_LOG.getFileExtension())) {
            return HoodieFileFormat.HOODIE_LOG;
        }
        if (extension.equals(HoodieFileFormat.ORC.getFileExtension())) {
            return HoodieFileFormat.ORC;
        }
        if (extension.equals(HoodieFileFormat.HFILE.getFileExtension())) {
            return HoodieFileFormat.HFILE;
        }
        throw new TrinoException(HUDI_UNSUPPORTED_FILE_FORMAT, "Hoodie InputFormat not implemented for base file of type " + extension);
    }

    private static String getFileExtension(String fullName)
    {
        String fileName = Location.of(fullName).fileName();
        int dotIndex = fileName.lastIndexOf('.');
        return dotIndex == -1 ? "" : fileName.substring(dotIndex);
    }

    public static boolean hudiMetadataExists(TrinoFileSystem trinoFileSystem, Location baseLocation)
    {
        try {
            Location metaLocation = baseLocation.appendPath(HoodieTableMetaClient.METAFOLDER_NAME);
            FileIterator iterator = trinoFileSystem.listFiles(metaLocation);
            // If there is at least one file in the .hoodie directory, it's a valid Hudi table
            return iterator.hasNext();
        }
        catch (IOException e) {
            throw new TrinoException(HUDI_FILESYSTEM_ERROR, "Failed to check for Hudi table at location: " + baseLocation, e);
        }
    }

    public static boolean partitionMatchesPredicates(
            SchemaTableName tableName,
            String hivePartitionName,
            List<HiveColumnHandle> partitionColumnHandles,
            List<String> partitionValues,
            TupleDomain<HiveColumnHandle> constraintSummary)
    {
        HivePartition partition = parsePartition(
                tableName, hivePartitionName, partitionColumnHandles, partitionValues);

        return partitionMatches(partitionColumnHandles, constraintSummary, partition);
    }

    /**
     * Copied from {@link io.trino.plugin.hive.HivePartitionManager#parsePartition}
     * to keep partition parsing logic self-contained within {@code trino-hudi}.
     */
    private static HivePartition parsePartition(
            SchemaTableName tableName,
            String partitionName,
            List<HiveColumnHandle> partitionColumns,
            List<String> partitionValues)
    {
        ImmutableMap.Builder<ColumnHandle, NullableValue> builder = ImmutableMap.builderWithExpectedSize(partitionColumns.size());
        for (int i = 0; i < partitionColumns.size(); i++) {
            HiveColumnHandle column = partitionColumns.get(i);
            NullableValue parsedValue = parsePartitionValue(partitionName, partitionValues.get(i), column.getType());
            builder.put(column, parsedValue);
        }
        Map<ColumnHandle, NullableValue> values = builder.buildOrThrow();
        return new HivePartition(tableName, partitionName, values);
    }

    public static boolean partitionMatches(List<HiveColumnHandle> partitionColumns, TupleDomain<HiveColumnHandle> constraintSummary, HivePartition partition)
    {
        if (constraintSummary.isNone()) {
            return false;
        }
        Map<HiveColumnHandle, Domain> domains = constraintSummary.getDomains().orElseGet(ImmutableMap::of);
        for (HiveColumnHandle column : partitionColumns) {
            NullableValue value = partition.getKeys().get(column);
            Domain allowedDomain = domains.get(column);
            if (allowedDomain != null && !allowedDomain.includesNullableValue(value.getValue())) {
                return false;
            }
        }
        return true;
    }

    public static List<HivePartitionKey> buildPartitionKeys(List<HiveColumnHandle> keys, List<String> values)
    {
        checkCondition(keys.size() == values.size(), HIVE_INVALID_METADATA,
                "Expected %s partition key values, but got %s. Keys: %s, Values: %s.",
                keys.size(), values.size(), keys, values);
        ImmutableList.Builder<HivePartitionKey> partitionKeys = ImmutableList.builder();
        for (int i = 0; i < keys.size(); i++) {
            String name = keys.get(i).getName();
            String value = values.get(i);
            partitionKeys.add(new HivePartitionKey(name, value));
        }
        return partitionKeys.build();
    }

    public static HoodieTableMetaClient buildTableMetaClient(
            TrinoFileSystem fileSystem,
            String tableName,
            String basePath)
    {
        try {
            return HoodieTableMetaClient.builder()
                    .setStorage(new HudiTrinoStorage(fileSystem, new TrinoStorageConfiguration()))
                    .setBasePath(basePath)
                    .build();
        }
        catch (TableNotFoundException e) {
            throw new TrinoException(HUDI_BAD_DATA,
                    "Location of table %s does not contain Hudi table metadata: %s".formatted(tableName, basePath));
        }
        catch (Throwable e) {
            throw new TrinoException(HUDI_META_CLIENT_ERROR,
                    "Unable to load Hudi meta client for table %s (%s)".formatted(tableName, basePath));
        }
    }

    public static Schema constructSchema(List<String> columnNames, List<HiveType> columnTypes)
    {
        // Convert lists into the format expected by the utility class
        String columnNamesString = String.join(",", columnNames);
        String columnTypesString = columnTypes.stream()
                .map(HiveType::getHiveTypeName)
                .map(Object::toString)
                .collect(Collectors.joining(":"));

        // Create the properties map
        Map<String, String> properties = new HashMap<>();
        properties.put(LIST_COLUMNS, columnNamesString);
        properties.put(LIST_COLUMN_TYPES, columnTypesString);

        // Call the public static method to build the schema
        try {
            // Pass null for the file system as we are not reading from a URL
            return AvroHiveFileUtils.determineSchemaOrThrowException(null, properties);
        }
        catch (IOException e) {
            // The IOException is declared on the method, but this path shouldn't throw it
            throw new UncheckedIOException("Failed to construct Avro schema", e);
        }
    }

    public static Schema constructSchema(Schema dataSchema, List<String> columnNames)
    {
        SchemaBuilder.RecordBuilder<Schema> schemaBuilder = SchemaBuilder.record("baseRecord");
        SchemaBuilder.FieldAssembler<Schema> fieldBuilder = schemaBuilder.fields();
        for (String columnName : columnNames) {
            Schema.Field field = getFieldFromSchema(columnName, dataSchema);
            Schema originalFieldSchema = field.schema();

            Schema typeForNewField;

            // Check if the original field schema is already nullable (i.e., a UNION containing NULL)
            if (originalFieldSchema.isNullable()) {
                typeForNewField = originalFieldSchema;
            }
            else {
                typeForNewField = Schema.createUnion(Schema.create(Schema.Type.NULL), originalFieldSchema);
            }

            fieldBuilder = fieldBuilder
                    .name(field.name())
                    .type(typeForNewField)
                    .withDefault(null);
        }
        return fieldBuilder.endRecord();
    }

    private static Map<String, Schema.Field> buildFieldLookup(Schema schema)
    {
        return schema.getFields().stream()
                .collect(Collectors.toMap(
                        f -> f.name().toLowerCase(Locale.ROOT),
                        f -> f));
    }

    /**
     * Retrieves a field from the given Avro schema by column name.
     * <p>
     * The lookup proceeds in two steps:
     * <ul>
     *   <li>First, attempts an exact match on the column name.</li>
     *   <li>If not found, falls back to a case-insensitive match using a cached lookup table</li>
     * </ul>
     *
     * @param columnName Column name to search for.
     * @param schema Avro {@link Schema} in which to search.
     * @return The matching {@link Schema.Field}, if found.
     * @throws TrinoException if no field matches the given column name.
     */
    public static Schema.Field getFieldFromSchema(String columnName, Schema schema)
    {
        Schema.Field field = schema.getField(columnName);
        if (field != null) {
            return field;
        }

        try {
            field = SCHEMA_FIELD_CACHE
                    .get(schema, () -> buildFieldLookup(schema)).get(columnName.toLowerCase(Locale.ROOT));
            if (field != null) {
                return field;
            }
        }
        catch (ExecutionException e) {
            throw new TrinoException(HUDI_SCHEMA_ERROR,
                    "Failed to build field lookup for schema", e);
        }

        throw new TrinoException(HUDI_SCHEMA_ERROR,
                "Failed to get column " + columnName + " from table schema");
    }

    public static List<HiveColumnHandle> prependHudiMetaAndOrderingColumns(HudiTableHandle tableHandle, List<HiveColumnHandle> dataColumns)
    {
        Set<String> existingColumns = dataColumns.stream()
                .map(HiveColumnHandle::getName)
                .collect(Collectors.toCollection(HashSet::new));

        List<HiveColumnHandle> columns = new ArrayList<>();

        // Add missing Hudi meta columns first
        for (int i = 0; i < HUDI_REQUIRED_META_COLUMNS.size(); i++) {
            String metaColumn = HUDI_REQUIRED_META_COLUMNS.get(i);
            if (existingColumns.add(metaColumn)) { // add() returns false if already present
                columns.add(new HiveColumnHandle(
                        metaColumn,
                        i,
                        HiveType.HIVE_STRING,
                        VarcharType.VARCHAR,
                        Optional.empty(),
                        HiveColumnHandle.ColumnType.REGULAR,
                        Optional.empty()));
            }
        }

        // Add missing ordering columns next
        tableHandle.getOrderingColumns().stream()
                .filter(col -> existingColumns.add(col.getName()))
                .forEach(columns::add);

        // Add all the original data columns after the new meta columns
        columns.addAll(dataColumns);

        return columns;
    }

    public static FileSlice convertToFileSlice(HudiSplit split, String basePath)
    {
        String dataFilePath = split.getBaseFile().isPresent()
                ? split.getBaseFile().get().getPath()
                : split.getLogFiles().getFirst().getPath();
        String fileId = FSUtils.getFileIdFromFileName(new StoragePath(dataFilePath).getName());
        HoodieBaseFile baseFile = split.getBaseFile().isPresent()
                ? new HoodieBaseFile(dataFilePath, fileId, split.getCommitTime(), null)
                : null;

        return new FileSlice(
                new HoodieFileGroupId(FSUtils.getRelativePartitionPath(new StoragePath(basePath), new StoragePath(dataFilePath)), fileId),
                split.getCommitTime(),
                baseFile,
                split.getLogFiles().stream().map(lf -> new HoodieLogFile(lf.getPath())).toList());
    }

    public static HoodieTableFileSystemView getFileSystemView(
            HoodieTableMetadata tableMetadata,
            HoodieTableMetaClient metaClient)
    {
        return new HoodieTableFileSystemView(
                tableMetadata, metaClient, metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants());
    }

    public static HoodieSchema getLatestTableSchema(HoodieTableMetaClient metaClient, String tableName)
    {
        try {
            HoodieTimer timer = HoodieTimer.start();
            HoodieSchema schema = new TableSchemaResolver(metaClient).getTableSchema();
            log.info("Fetched table schema for table %s in %s ms", tableName, timer.endTimer());
            return schema;
        }
        catch (Exception e) {
            // failed to read schema
            throw new TrinoException(HUDI_FILESYSTEM_ERROR, e);
        }
    }

    /**
     * Returns the column handles that must be present in the read schema for the file group reader to merge
     * correctly: the ordering columns, plus the mandatory merge columns declared by a configured custom record
     * merger (via {@link HoodieRecordMerger#getMandatoryFieldsForMerging}).
     * <p>
     * For COMMIT_TIME / EVENT_TIME tables this is exactly the ordering columns (so behavior is unchanged). For a
     * CUSTOM merge mode with a registered merger, it additionally includes any data columns the merger reads at
     * merge time (e.g. an arbitrary decision column) so that those columns are read from the base file even when
     * the query does not project them -- without this the merger would see null for an un-projected column.
     */
    public static List<HiveColumnHandle> getMergeRequiredColumnHandles(
            Table table,
            TypeManager typeManager,
            Lazy<HoodieTableMetaClient> lazyMetaClient,
            List<String> recordMergerImpls,
            HiveTimestampPrecision timestampPrecision)
    {
        HoodieTableMetaClient metaClient = lazyMetaClient.get();
        HoodieTableConfig tableConfig = metaClient.getTableConfig();
        RecordMergeMode recordMergeMode = tableConfig.getRecordMergeMode();

        LinkedHashSet<String> requiredColumnNames = new LinkedHashSet<>();
        if (recordMergeMode != null && recordMergeMode != RecordMergeMode.COMMIT_TIME_ORDERING) {
            requiredColumnNames.addAll(tableConfig.getOrderingFields());
        }

        // For a CUSTOM merge mode, ask the configured merger which fields it needs at merge time and include them
        // so they are read even when not projected. Only the merger's declared columns are added (not all columns),
        // so non-custom tables and mergers that only use the key/ordering fields incur no extra reads.
        if (recordMergeMode == RecordMergeMode.CUSTOM && recordMergerImpls != null && !recordMergerImpls.isEmpty()) {
            Option<HoodieRecordMerger> merger = HoodieRecordUtils.createValidRecordMerger(
                    EngineType.JAVA, String.join(",", recordMergerImpls), tableConfig.getRecordMergeStrategyId());
            if (merger.isPresent()) {
                TypedProperties props = new TypedProperties();
                props.putAll(tableConfig.getProps());
                props.setProperty(RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY, String.join(",", recordMergerImpls));
                HoodieSchema tableSchema;
                try {
                    tableSchema = new TableSchemaResolver(metaClient).getTableSchema();
                }
                catch (Exception e) {
                    throw new TrinoException(HUDI_SCHEMA_ERROR, "Failed to resolve table schema for merge column resolution", e);
                }
                String[] mandatoryFields = merger.get().getMandatoryFieldsForMerging(tableSchema, tableConfig, props);
                if (mandatoryFields != null) {
                    Collections.addAll(requiredColumnNames, mandatoryFields);
                }
            }
        }

        if (requiredColumnNames.isEmpty()) {
            return Collections.emptyList();
        }
        return buildColumnHandles(table, typeManager, requiredColumnNames, timestampPrecision);
    }

    /**
     * Builds {@link HiveColumnHandle}s, preserving physical (data-column) index, for the data columns whose names
     * appear in {@code columnNames}. Names that are not data columns (e.g. Hudi meta fields) or whose types are not
     * supported by the storage format are skipped.
     */
    private static List<HiveColumnHandle> buildColumnHandles(Table table, TypeManager typeManager, Set<String> columnNames, HiveTimestampPrecision timestampPrecision)
    {
        ImmutableList.Builder<HiveColumnHandle> columns = ImmutableList.builder();
        int hiveColumnIndex = 0;
        for (Column field : table.getDataColumns()) {
            if (columnNames.contains(field.getName())) {
                HiveType hiveType = field.getType();
                // ignore unsupported types rather than failing
                if (typeSupported(hiveType.getTypeInfo(), table.getStorage().getStorageFormat())) {
                    columns.add(createBaseColumn(field.getName(), hiveColumnIndex, hiveType, getType(hiveType, typeManager, timestampPrecision), REGULAR, field.getComment()));
                }
            }
            hiveColumnIndex++;
        }
        return columns.build();
    }

    /**
     * Converts the given {@link HoodiePairData} into a {@link Map}.
     * <p>
     * Special handling is applied for null keys:
     * <ul>
     *   <li>If a key is null, it is stored in the map as a {@code null} entry.</li>
     *   <li>If multiple entries share the same key (including null), the latest value overwrites the previous one.</li>
     * </ul>
     *
     * @param pairData the HoodiePairData containing key-value pairs
     * @param <K> the type of keys maintained by the resulting map
     * @param <V> the type of mapped values
     * @return a {@link Map} containing all key-value pairs from the input data
     */
    public static <K, V> Map<K, V> collectAsMap(HoodiePairData<K, V> pairData)
    {
        // HashMap allows null keys, so put directly; on duplicate keys the later entry wins.
        Map<K, V> result = new HashMap<>();
        pairData.collectAsList().forEach(pair -> result.put(pair.getKey(), pair.getValue()));
        return result;
    }
}
