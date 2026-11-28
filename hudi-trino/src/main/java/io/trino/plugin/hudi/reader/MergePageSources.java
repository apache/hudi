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
package io.trino.plugin.hudi.reader;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hudi.HudiUtil;
import io.trino.plugin.hudi.reader.HudiTrinoReaderContext.LogFileParquetPageSourceFactory;
import io.trino.plugin.hudi.util.TrinoTypeMapper;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.inline.InLineFSUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.hudi.HudiErrorCode.HUDI_SCHEMA_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * The page sources a merge read reads files through, shared by {@link HudiTrinoReaderContext} and
 * {@link HudiTrinoRowReaderContext}: the pre-built base page source, and a page source built on demand for each
 * native (RFC-103) delta-log parquet file.
 */
final class MergePageSources
{
    private final ConnectorPageSource basePageSource;
    private final List<HiveColumnHandle> baseProjection;
    private final LogFileParquetPageSourceFactory logPageSourceFactory;
    private final Map<String, HiveColumnHandle> colNameToHandle;
    private final Set<String> baseProjectionNames;
    // Present when hudi.schema-column-types.enabled is on: the projection then carries TrinoTypeMapper types
    private final Optional<TrinoTypeMapper> schemaTypeMapper;

    /**
     * The page source of a file and the column handles of its channels.
     */
    record FilePageSource(ConnectorPageSource pageSource, List<HiveColumnHandle> projection) {}

    /**
     * Turns one page position into a record.
     */
    @FunctionalInterface
    interface PositionReader<T>
    {
        T read(Page page, int position);
    }

    MergePageSources(
            ConnectorPageSource basePageSource,
            List<HiveColumnHandle> baseProjection,
            LogFileParquetPageSourceFactory logPageSourceFactory,
            Optional<TrinoTypeMapper> schemaTypeMapper)
    {
        this.basePageSource = basePageSource;
        this.baseProjection = baseProjection;
        this.logPageSourceFactory = logPageSourceFactory;
        this.schemaTypeMapper = requireNonNull(schemaTypeMapper, "schemaTypeMapper is null");
        this.colNameToHandle = new HashMap<>();
        for (HiveColumnHandle handle : baseProjection) {
            colNameToHandle.put(handle.getBaseColumnName().toLowerCase(Locale.ROOT), handle);
        }
        // Immutable snapshot of the read projection for the base-read guard in open; colNameToHandle cannot serve
        // that purpose because buildRequiredColumnHandles adds log-side handles to it on demand.
        this.baseProjectionNames = ImmutableSet.copyOf(colNameToHandle.keySet());
    }

    /**
     * Opens the given file projected on {@code requiredSchema}. For a native (RFC-103) delta-log parquet file a fresh
     * page source is built on demand with predicate pushdown disabled so every log record is read and merged; for the
     * base file the pre-built base page source is reused. Classic Avro log blocks never reach here (they deserialize
     * inline).
     * <p>
     * Either way every {@code requiredSchema} field is a channel of the returned projection, matched by name
     * case-insensitively. Records read from it must CARRY {@code requiredSchema}: the file-group reader tracks that
     * schema for every buffered record, and payload-based merging round-trips records through Avro binary with it
     * ({@code BaseAvroPayload}), so a record whose own schema differs (in field order or nullability) would decode into
     * garbage values there.
     */
    FilePageSource open(StoragePath path, long start, long length, HoodieSchema requiredSchema)
    {
        if (FSUtils.isLogFile(path)) {
            // Inline parquet log blocks (inlinefs:// scheme) need a separate inline-aware reader; out of scope here.
            if (InLineFSUtils.SCHEME.equals(path.toUri().getScheme())) {
                throw new UnsupportedOperationException("Inline log blocks are not supported by the Hudi Trino connector: " + path);
            }
            List<HiveColumnHandle> logProjection = buildRequiredColumnHandles(requiredSchema);
            return new FilePageSource(logPageSourceFactory.create(path.toString(), start, length, logProjection), logProjection);
        }
        // The base read reuses the pre-built page source, so it can only satisfy requiredSchema fields the
        // read projection carries. The connector predicts the file-group reader's demands up front
        // (HudiUtil.getMergeRequiredColumnHandles, HudiPageSourceProvider.requiresFullSchemaRead); if the
        // two ever drift, fail loudly here instead of silently merging with null column values.
        List<String> missingColumns = requiredSchema.getFields().stream()
                .map(HoodieSchemaField::name)
                .filter(name -> !baseProjectionNames.contains(name.toLowerCase(Locale.ROOT)))
                .toList();
        if (!missingColumns.isEmpty()) {
            throw new TrinoException(HUDI_SCHEMA_ERROR, format(
                    "The file-group reader requires columns %s for merging, but the base-file read projection "
                            + "does not carry them. The connector's merge projection is out of sync with "
                            + "FileGroupReaderSchemaHandler.generateRequiredSchema.",
                    missingColumns));
        }
        // Every requiredSchema field is in the base projection (checked above) and every projected column is
        // a requiredSchema field (the file-group reader's required schema always contains the full requested
        // schema), so the by-name channel mapping resolves in both directions.
        return new FilePageSource(basePageSource, baseProjection);
    }

    /**
     * Resolves the {@link HiveColumnHandle} for each field of {@code requiredSchema} so the on-demand log
     * page source reads exactly the columns the file-group reader needs to merge. Fields the connector
     * projection carries resolve to their projection handles (planner-authoritative types); the file-group
     * reader can also require fields the projection does not carry (e.g. {@code _hoodie_commit_time} or a
     * delete-marker column on a narrow query) -- every such field originates from the table schema and
     * carries its real Avro type, so its handle is typed directly from the field and cached.
     */
    private List<HiveColumnHandle> buildRequiredColumnHandles(HoodieSchema requiredSchema)
    {
        List<HiveColumnHandle> handles = new ArrayList<>();
        for (HoodieSchemaField field : requiredSchema.getFields()) {
            // A projection handle may carry the lowercased metastore column name (e.g. 'op' for the
            // DMS delete key 'Op'); that is fine on this path: parquet columns resolve
            // case-insensitively, records map channels into requiredSchema positions
            // case-insensitively, and hudi-common reads merge fields off the record's own schema,
            // which carries the table casing.
            handles.add(colNameToHandle.computeIfAbsent(
                    field.name().toLowerCase(Locale.ROOT),
                    _ -> HudiUtil.toColumnHandle(field, schemaTypeMapper)));
        }
        return handles;
    }

    /**
     * Iterates the positions of every page of {@code source}, closing the source when the iterator is closed.
     */
    static <T> ClosableIterator<T> recordIterator(ConnectorPageSource source, PositionReader<T> reader)
    {
        return new ClosableIterator<>()
        {
            private Page currentPage;
            private int currentPosition;

            @Override
            public void close()
            {
                try {
                    source.close();
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public boolean hasNext()
            {
                // If all records in the current page are consume, try to get next page
                if (currentPage == null || currentPosition >= currentPage.getPositionCount()) {
                    if (source.isFinished()) {
                        return false;
                    }

                    // Get next page and reset currentPosition. Unwrap the SourcePage to the
                    // underlying Page so the reader's Block accessors keep working.
                    SourcePage nextSourcePage = source.getNextSourcePage();
                    currentPage = nextSourcePage == null ? null : nextSourcePage.getPage();
                    currentPosition = 0;

                    // If no more pages are available
                    return currentPage != null;
                }

                return true;
            }

            @Override
            public T next()
            {
                if (!hasNext()) {
                    throw new NoSuchElementException("No more records in the iterator");
                }

                T record = reader.read(currentPage, currentPosition);
                currentPosition++;
                return record;
            }
        };
    }
}
