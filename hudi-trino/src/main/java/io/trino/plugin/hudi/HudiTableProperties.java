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
import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeManager;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;
import static java.util.Objects.requireNonNull;

public class HudiTableProperties
{
    public static final String LOCATION_PROPERTY = "location";
    public static final String PARTITIONED_BY_PROPERTY = "partitioned_by";
    public static final String TABLE_TYPE_PROPERTY = "table_type";
    public static final String PRIMARY_KEY_PROPERTY = "primary_key";
    public static final String ORDERING_FIELDS_PROPERTY = "ordering_fields";
    public static final String PRECOMBINE_FIELD_PROPERTY = "precombine_field";
    public static final String RECORD_MERGE_MODE_PROPERTY = "record_merge_mode";
    public static final String KEY_GENERATOR_CLASS_PROPERTY = "key_generator_class";
    public static final String HIVE_STYLE_PARTITIONING_PROPERTY = "hive_style_partitioning";
    public static final String HOODIE_PROPERTIES_PROPERTY = "hoodie_properties";

    /**
     * The {@code hoodie.*} configs that may be passed through to {@code hoodie.properties}.
     * <p>
     * Deliberately not "any {@code hoodie.*} config": {@code HoodieTableMetaClient.TableBuilder#set}
     * filters what it persists against {@link HoodieTableConfig#PERSISTED_CONFIG_LIST}, and silently
     * drops everything else. That list is currently the nine {@code hoodie.keygen.timebased.*}
     * configs a timestamp key generator needs. Validating against it here turns a config that would
     * have vanished without trace into an error at parse time, before any storage is touched.
     */
    private static final Set<String> PERSISTABLE_CONFIG_KEYS = HoodieTableConfig.PERSISTED_CONFIG_LIST.stream()
            .map(ConfigProperty::key)
            .collect(toImmutableSet());

    private final List<PropertyMetadata<?>> tableProperties;

    @Inject
    public HudiTableProperties(TypeManager typeManager)
    {
        requireNonNull(typeManager, "typeManager is null");
        tableProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(stringProperty(
                        LOCATION_PROPERTY,
                        "File system location URI for the table",
                        null,
                        false))
                .add(new PropertyMetadata<>(
                        PARTITIONED_BY_PROPERTY,
                        "Partition columns",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ((Collection<String>) value).stream()
                                .map(name -> name.toLowerCase(ENGLISH))
                                .collect(toImmutableList()),
                        value -> value))
                .add(enumProperty(
                        TABLE_TYPE_PROPERTY,
                        "Hudi table type",
                        HoodieTableType.class,
                        HoodieTableType.COPY_ON_WRITE,
                        false))
                .add(new PropertyMetadata<>(
                        PRIMARY_KEY_PROPERTY,
                        "Columns that uniquely identify a record, written as " + HoodieTableConfig.RECORDKEY_FIELDS.key(),
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ((Collection<String>) value).stream()
                                .map(name -> name.toLowerCase(ENGLISH))
                                .collect(toImmutableList()),
                        value -> value))
                // Hudi renamed the precombine field to the plural "ordering fields" in 1.0.0;
                // hoodie.table.precombine.field survives only as a deprecated alternative of
                // hoodie.table.ordering.fields. Spark SQL made the same move (preCombineField ->
                // orderingFields), so this follows the current name rather than the legacy one.
                .add(new PropertyMetadata<>(
                        ORDERING_FIELDS_PROPERTY,
                        "Columns used to order records during merging, written as " + HoodieTableConfig.ORDERING_FIELDS.key(),
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ((Collection<String>) value).stream()
                                .map(name -> name.toLowerCase(ENGLISH))
                                .collect(toImmutableList()),
                        value -> value))
                // Deprecated alias kept for parity with Spark, which still accepts preCombineField
                // through HoodieOptionConfig's withAlternatives, and so that the CREATE TABLE example
                // in the connector's own design discussion works verbatim. Singular and a plain
                // string, matching the shape Spark and that example use. Hidden so it is not
                // advertised alongside the current name; resolution happens in getOrderingFields.
                .add(stringProperty(
                        PRECOMBINE_FIELD_PROPERTY,
                        "Deprecated. Use " + ORDERING_FIELDS_PROPERTY + " instead",
                        null,
                        true))
                // Left unset by default: Hudi derives the merge mode from whether ordering fields
                // are present (COMMIT_TIME_ORDERING when absent, EVENT_TIME_ORDERING when set).
                // Pinning a default here would override that inference.
                .add(enumProperty(
                        RECORD_MERGE_MODE_PROPERTY,
                        "Record merge mode",
                        RecordMergeMode.class,
                        null,
                        false))
                // Also left unset: when absent, the engine that first writes to the table picks its
                // own default key generator from the partition fields. Writing a value here would
                // pin a choice Trino cannot itself act on, since Stage 1 never writes records.
                .add(stringProperty(
                        KEY_GENERATOR_CLASS_PROPERTY,
                        "Fully qualified key generator class name",
                        null,
                        false))
                .add(booleanProperty(
                        HIVE_STYLE_PARTITIONING_PROPERTY,
                        "Use Hive-style partition paths (key=value)",
                        null,
                        false))
                .add(new PropertyMetadata<>(
                        HOODIE_PROPERTIES_PROPERTY,
                        "Additional hoodie.* table configs written to hoodie.properties; limited to the configs Hudi persists (hoodie.keygen.timebased.*)",
                        new MapType(VARCHAR, VARCHAR, typeManager.getTypeOperators()),
                        Map.class,
                        ImmutableMap.of(),
                        // Hidden for the same reason the Hive connector hides extra_properties:
                        // getTableMetadata does not read hoodie.properties, so this cannot be
                        // round-tripped through SHOW CREATE TABLE yet.
                        true,
                        value -> {
                            Map<String, String> properties = (Map<String, String>) value;
                            properties.forEach((key, propertyValue) -> {
                                if (!PERSISTABLE_CONFIG_KEYS.contains(key)) {
                                    throw new TrinoException(INVALID_TABLE_PROPERTY, format(
                                            "Hudi does not persist '%s' in hoodie.properties, so it would have no effect. "
                                                    + "%s accepts only: %s",
                                            key,
                                            HOODIE_PROPERTIES_PROPERTY,
                                            PERSISTABLE_CONFIG_KEYS.stream().sorted().collect(joining(", "))));
                                }
                                if (propertyValue == null) {
                                    throw new TrinoException(INVALID_TABLE_PROPERTY, format(
                                            "Value in %s cannot be null for key '%s'", HOODIE_PROPERTIES_PROPERTY, key));
                                }
                            });
                            return ImmutableMap.copyOf(properties);
                        },
                        value -> value))
                .build();
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    public static Optional<String> getTableLocation(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((String) tableProperties.get(LOCATION_PROPERTY));
    }

    @SuppressWarnings("unchecked")
    public static List<String> getPartitionedBy(Map<String, Object> tableProperties)
    {
        return (List<String>) tableProperties.getOrDefault(PARTITIONED_BY_PROPERTY, ImmutableList.of());
    }

    public static HoodieTableType getTableType(Map<String, Object> tableProperties)
    {
        return (HoodieTableType) tableProperties.getOrDefault(TABLE_TYPE_PROPERTY, HoodieTableType.COPY_ON_WRITE);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getPrimaryKey(Map<String, Object> tableProperties)
    {
        return (List<String>) tableProperties.getOrDefault(PRIMARY_KEY_PROPERTY, ImmutableList.of());
    }

    /**
     * Resolves the ordering fields, accepting the deprecated {@code precombine_field} alias.
     * <p>
     * Both names map to {@code hoodie.table.ordering.fields}, so setting both is rejected rather
     * than silently preferring one.
     */
    @SuppressWarnings("unchecked")
    public static List<String> getOrderingFields(Map<String, Object> tableProperties)
    {
        List<String> orderingFields = (List<String>) tableProperties.getOrDefault(ORDERING_FIELDS_PROPERTY, ImmutableList.of());
        String precombineField = (String) tableProperties.get(PRECOMBINE_FIELD_PROPERTY);
        if (precombineField == null) {
            return orderingFields;
        }
        if (!orderingFields.isEmpty()) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, format(
                    "Cannot set both %s and %s; they both write %s. Use %s.",
                    ORDERING_FIELDS_PROPERTY,
                    PRECOMBINE_FIELD_PROPERTY,
                    HoodieTableConfig.ORDERING_FIELDS.key(),
                    ORDERING_FIELDS_PROPERTY));
        }
        return ImmutableList.of(precombineField.toLowerCase(ENGLISH));
    }

    public static Optional<RecordMergeMode> getRecordMergeMode(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((RecordMergeMode) tableProperties.get(RECORD_MERGE_MODE_PROPERTY));
    }

    public static Optional<String> getKeyGeneratorClass(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((String) tableProperties.get(KEY_GENERATOR_CLASS_PROPERTY));
    }

    public static Optional<Boolean> getHiveStylePartitioning(Map<String, Object> tableProperties)
    {
        return Optional.ofNullable((Boolean) tableProperties.get(HIVE_STYLE_PARTITIONING_PROPERTY));
    }

    @SuppressWarnings("unchecked")
    public static Map<String, String> getHoodieProperties(Map<String, Object> tableProperties)
    {
        return (Map<String, String>) tableProperties.getOrDefault(HOODIE_PROPERTIES_PROPERTY, ImmutableMap.of());
    }
}
