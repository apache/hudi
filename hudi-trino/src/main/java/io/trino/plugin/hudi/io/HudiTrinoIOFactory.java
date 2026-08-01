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
package io.trino.plugin.hudi.io;

import org.apache.hudi.common.fs.ConsistencyGuard;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.FileFormatUtils;
import org.apache.hudi.common.util.HFileUtils;
import org.apache.hudi.core.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.core.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

public class HudiTrinoIOFactory
        extends HoodieIOFactory
{
    public HudiTrinoIOFactory(HoodieStorage storage)
    {
        super(storage);
    }

    @Override
    public HoodieFileReaderFactory getReaderFactory(HoodieRecord.HoodieRecordType recordType)
    {
        return new HudiTrinoFileReaderFactory(storage);
    }

    @Override
    public HoodieFileWriterFactory getWriterFactory(HoodieRecord.HoodieRecordType recordType)
    {
        throw new UnsupportedOperationException("HudiTrinoIOFactory does not support writers.");
    }

    @Override
    public FileFormatUtils getFileFormatUtils(HoodieFileFormat fileFormat)
    {
        if (fileFormat == HoodieFileFormat.PARQUET) {
            // Parquet needs a Trino-native implementation: hudi's own ParquetUtils lives in
            // hudi-hadoop-common, which is excluded from the Trino runtime.
            return new HudiTrinoParquetFileFormatUtils();
        }
        if (fileFormat == HoodieFileFormat.HFILE) {
            // hudi-common's HFileUtils is hadoop-free (it decodes via hudi-io's native HFile
            // reader), so it is safe in the Trino runtime. This is what lets the connector read
            // uncompacted metadata-table deltas, which are native HFILE log files.
            return new HFileUtils();
        }
        throw new UnsupportedOperationException(
                "Native " + fileFormat + " log files are not supported by the Hudi Trino connector");
    }

    @Override
    public HoodieStorage getStorage(StoragePath storagePath)
    {
        return storage;
    }

    @Override
    public HoodieStorage getStorage(StoragePath path, boolean enableRetry, long maxRetryIntervalMs, int maxRetryNumbers, long initialRetryIntervalMs, String retryExceptions, ConsistencyGuard consistencyGuard)
    {
        return storage;
    }
}
