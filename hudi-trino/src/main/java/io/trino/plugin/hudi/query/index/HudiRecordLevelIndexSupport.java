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
package io.trino.plugin.hudi.query.index;

import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.plugin.hudi.util.TupleDomainUtils;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.common.util.Lazy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.trino.plugin.hudi.HudiErrorCode.HUDI_BAD_DATA;
import static io.trino.plugin.hudi.HudiSessionProperties.getRecordIndexWaitTimeout;
import static io.trino.plugin.hudi.HudiUtil.collectAsMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.hudi.keygen.KeyGenerator.EMPTY_RECORDKEY_PLACEHOLDER;

public class HudiRecordLevelIndexSupport
        extends HudiBaseIndexSupport
{
    private static final Logger log = Logger.get(HudiRecordLevelIndexSupport.class);

    public static final String DEFAULT_COLUMN_VALUE_SEPARATOR = ":";
    public static final String DEFAULT_RECORD_KEY_PARTS_SEPARATOR = ",";
    private final CompletableFuture<Optional<Set<String>>> relevantFileIdsFuture;
    private final Duration recordIndexWaitTimeout;
    private final long futureStartTimeMs;

    public HudiRecordLevelIndexSupport(ConnectorSession session, SchemaTableName schemaTableName, Lazy<HoodieTableMetaClient> lazyMetaClient, Lazy<HoodieTableMetadata> lazyTableMetadata, TupleDomain<String> regularColumnPredicates, HoodieSchema tableSchema)
    {
        super(log, schemaTableName, lazyMetaClient);
        this.recordIndexWaitTimeout = getRecordIndexWaitTimeout(session);
        if (regularColumnPredicates.isAll()) {
            log.debug("Predicates cover all data, skipping record level index lookup.");
            this.relevantFileIdsFuture = CompletableFuture.completedFuture(Optional.empty());
        }
        else {
            this.relevantFileIdsFuture = CompletableFuture.supplyAsync(() -> {
                HoodieTimer timer = HoodieTimer.start();
                Option<String[]> recordKeyFieldsOpt = lazyMetaClient.get().getTableConfig().getRecordKeyFields();
                if (recordKeyFieldsOpt.isEmpty() || recordKeyFieldsOpt.get().length == 0) {
                    // Should not happen since canApply checks for this, include for safety
                    throw new TrinoException(HUDI_BAD_DATA, "Record key fields must be defined to use Record Level Index.");
                }
                List<String> recordKeyFields = Arrays.asList(recordKeyFieldsOpt.get());

                // Only extract the predicates relevant to the record key fields
                TupleDomain<String> filteredDomains = extractPredicatesForColumns(regularColumnPredicates, recordKeyFields);

                // Construct the actual record keys based on the filtered predicates using Hudi's encoding scheme
                List<String> recordKeys = constructRecordKeys(filteredDomains, recordKeyFields, tableSchema, IndexKeyForm.RECORD_KEY);

                if (recordKeys.isEmpty()) {
                    // If key construction fails (e.g., incompatible predicates not caught by canApply, or placeholder issue)
                    log.warn("Took %s ms, but could not construct record keys from predicates. Skipping record index pruning for table %s",
                            timer.endTimer(), schemaTableName);
                    return Optional.empty();
                }
                log.debug(String.format("Constructed %d record keys for index lookup.", recordKeys.size()));

                // Perform index lookup in metadataTable
                // TODO: document here what this map is keyed by
                Map<String, HoodieRecordGlobalLocation> recordIndex = collectAsMap(lazyTableMetadata.get().readRecordIndexLocationsWithKeys(HoodieListData.eager(recordKeys)));
                if (recordIndex.isEmpty()) {
                    log.debug("Record level index lookup took %s ms but returned no locations for the given keys %s for table %s",
                            timer.endTimer(), recordKeys, schemaTableName);
                    // Return all original fileSlices
                    return Optional.empty();
                }

                // Collect fileIds for pruning
                Set<String> relevantFiles = recordIndex.values().stream()
                        .map(HoodieRecordGlobalLocation::getFileId)
                        .collect(Collectors.toSet());
                log.debug("Record level index lookup took %s ms and identified %d relevant file IDs.", timer.endTimer(), relevantFiles.size());

                return Optional.of(relevantFiles);
            });
        }
        this.futureStartTimeMs = System.currentTimeMillis();
    }

    @Override
    public boolean shouldSkipFileSlice(FileSlice slice)
    {
        try {
            if (relevantFileIdsFuture.isDone()) {
                Optional<Set<String>> relevantFileIds = relevantFileIdsFuture.get();
                return relevantFileIds.map(fileIds -> !fileIds.contains(slice.getFileId())).orElse(false);
            }

            long elapsedMs = System.currentTimeMillis() - futureStartTimeMs;
            if (elapsedMs > recordIndexWaitTimeout.toMillis()) {
                // Took too long; skip decision
                return false;
            }

            long remainingMs = Math.max(0, recordIndexWaitTimeout.toMillis() - elapsedMs);
            Optional<Set<String>> relevantFileIds = relevantFileIdsFuture.get(remainingMs, MILLISECONDS);
            return relevantFileIds.map(fileIds -> !fileIds.contains(slice.getFileId())).orElse(false);
        }
        catch (TimeoutException | InterruptedException | ExecutionException e) {
            return false;
        }
    }

    /**
     * Checks if the Record Level Index is available and the query predicates
     * reference all record key fields with compatible (IN/EQUAL) constraints.
     */
    @Override
    public boolean canApply(TupleDomain<String> tupleDomain)
    {
        if (!isIndexSupportAvailable()) {
            log.debug("Record Level Index partition is not enabled in metadata.");
            return false;
        }

        Option<String[]> recordKeyFieldsOpt = lazyMetaClient.get().getTableConfig().getRecordKeyFields();
        if (recordKeyFieldsOpt.isEmpty() || recordKeyFieldsOpt.get().length == 0) {
            log.debug("Record key fields are not defined in table config.");
            return false;
        }
        List<String> recordKeyFields = Arrays.asList(recordKeyFieldsOpt.get());

        // Ensure that predicates reference all record key fields and use IN/EQUAL
        boolean applicable = TupleDomainUtils.areAllFieldsReferenced(tupleDomain, recordKeyFields)
                && TupleDomainUtils.areDomainsInOrEqualOnly(tupleDomain, recordKeyFields);

        if (!applicable) {
            log.debug("Predicates do not reference all record key fields or use non-compatible (non IN/EQUAL) constraints.");
        }
        else {
            log.debug("Record Level Index is available and applicable based on predicates.");
        }
        return applicable;
    }

    private boolean isIndexSupportAvailable()
    {
        return lazyMetaClient.get().getTableConfig().getMetadataPartitions()
                .contains(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX);
    }

    /**
     * Extracts predicates from a TupleDomain that match a given set of columns.
     * Preserves all complex predicate properties including multi-value domains,
     * range-based predicates, and nullability.
     *
     * @param tupleDomain The source TupleDomain containing all predicates
     * @param columnFields The set of columns for which to extract predicates
     * @return A new TupleDomain containing only the predicates for the specified columns
     */
    public static TupleDomain<String> extractPredicatesForColumns(TupleDomain<String> tupleDomain, List<String> columnFields)
    {
        if (tupleDomain.isNone()) {
            return TupleDomain.none();
        }

        if (tupleDomain.isAll()) {
            return TupleDomain.all();
        }

        // Extract the domains matching the specified columns
        Map<String, Domain> allDomains = tupleDomain.getDomains().get();
        Map<String, Domain> filteredDomains = allDomains.entrySet().stream().filter(entry -> columnFields.contains(entry.getKey())) // Ensure key is in the column set
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        // If no domains matched, but we had some columns to extract, return ALL
        if (filteredDomains.isEmpty() && !columnFields.isEmpty() && !allDomains.isEmpty()) {
            return TupleDomain.all();
        }

        return TupleDomain.withColumnDomains(filteredDomains);
    }

    /**
     * Constructs the keys an index lookup needs from the EQUAL or IN domains of the key columns, a single-column key
     * as the value's key text and a composite one as {@code field:text} parts joined by commas, the form
     * {@code KeyGenerator#constructRecordKey} writes.
     * <p>
     * Each value's text comes from {@link IndexKeyForm#keyText}, driven by the key field's type in
     * {@code tableSchema}. An empty list, so no pruning, is returned when:
     * <ol>
     * <li>keyFields is empty, or keyDomains isAll</li>
     * <li>a key field has no domain, or a domain is not an EQUAL or IN domain</li>
     * <li>the table schema is unknown, or a key field's value has no key text (see {@link IndexKeyForm#keyText})</li>
     * </ol>
     * <p>
     * Note: This function is O(m^n) where m is the average size of value literals and n is the number of record keys.
     * <p>
     * Optimization 1: If MDT enabled functions allows for streams to be passed in, we can implement an iterator to be more memory efficient.
     * <p>
     * Optimization 2: We should also consider limiting the number of recordKeys generated, if it is estimated to be more than a limit, RLI should just be skipped
     * as it may just be faster to read out all data and filer accordingly.
     *
     * @param keyDomains The filtered TupleDomain containing column handles and values
     * @param keyFields List of column names that represent the key
     * @param tableSchema The table schema the key fields are typed from; null when it could not be resolved
     * @param keyForm How the index writes a value as key text
     * @return List of string values representing the key(s)
     */
    public static List<String> constructRecordKeys(TupleDomain<String> keyDomains, List<String> keyFields, HoodieSchema tableSchema, IndexKeyForm keyForm)
    {
        // TODO: Move this to TupleDomainUtils
        // If no recordKeys or no recordKeyDomains, return empty list
        if (keyFields == null || keyFields.isEmpty() || keyDomains.isAll() || tableSchema == null) {
            return Collections.emptyList();
        }

        // All recordKeys must have a domain else, return empty list (applicable to complexKeys)
        // If a one of the recordKey in the set of complexKeys does not have a domain, we are unable to construct
        // a complete complexKey
        Map<String, Domain> domains = keyDomains.getDomains().get();
        if (!domains.keySet().containsAll(keyFields)) {
            return Collections.emptyList();
        }

        List<List<String>> keyTexts = new ArrayList<>(keyFields.size());
        for (String keyField : keyFields) {
            Domain domain = domains.get(keyField);
            Optional<HoodieSchemaField> field = findField(tableSchema, keyField);
            if (field.isEmpty()) {
                return Collections.emptyList();
            }
            Optional<Function<Object, String>> keyText = keyForm.keyText(keyField, field.get().schema(), domain.getType());
            if (keyText.isEmpty()) {
                return Collections.emptyList();
            }
            List<String> values = extractStringValues(domain, keyText.get());
            if (values.isEmpty()) {
                return Collections.emptyList();
            }
            keyTexts.add(values);
        }

        // Case 1: Not a complex key (single record key)
        if (keyFields.size() == 1) {
            return keyTexts.getFirst();
        }
        // Case 2: Complex/Composite key (multiple record keys)
        // Create a queue to manage the Cartesian product generation
        Queue<String> results = new LinkedList<>();
        for (int i = 0; i < keyFields.size(); i++) {
            String keyField = keyFields.get(i);
            List<String> values = keyTexts.get(i);
            if (results.isEmpty()) {
                // First iteration: initialize the queue
                values.forEach(v -> results.offer(compositeKeyPart(keyField, v)));
            }
            else {
                int size = results.size();
                for (int j = 0; j < size; j++) {
                    String currentEntry = results.poll();

                    // Generate new combinations by appending keyParts to existing keyParts
                    for (String v : values) {
                        results.offer(currentEntry + DEFAULT_RECORD_KEY_PARTS_SEPARATOR + compositeKeyPart(keyField, v));
                    }
                }
            }
        }
        return results.stream().toList();
    }

    /**
     * A part of a composite key; an empty value is written as a placeholder, as {@code KeyGenerator#constructRecordKey}
     * does.
     */
    private static String compositeKeyPart(String keyField, String keyText)
    {
        return keyField + DEFAULT_COLUMN_VALUE_SEPARATOR + (keyText.isEmpty() ? EMPTY_RECORDKEY_PLACEHOLDER : keyText);
    }

    private static Optional<HoodieSchemaField> findField(HoodieSchema tableSchema, String name)
    {
        Option<HoodieSchemaField> field = tableSchema.getField(name);
        if (field.isPresent()) {
            return Optional.of(field.get());
        }
        return tableSchema.getFields().stream()
                .filter(candidate -> candidate.name().equalsIgnoreCase(name))
                .findFirst();
    }

    /**
     * The key texts of the values of an EQUAL or IN domain; empty for any other domain.
     */
    private static List<String> extractStringValues(Domain domain, Function<Object, String> keyText)
    {
        List<String> values = new ArrayList<>();

        if (domain.isSingleValue()) {
            // Handle EQUAL condition (single value domain)
            values.add(keyText.apply(domain.getSingleValue()));
        }
        else if (domain.getValues().isDiscreteSet()) {
            // Handle IN condition (set of discrete values)
            for (Object value : domain.getValues().getDiscreteSet()) {
                values.add(keyText.apply(value));
            }
        }
        return values;
    }
}
