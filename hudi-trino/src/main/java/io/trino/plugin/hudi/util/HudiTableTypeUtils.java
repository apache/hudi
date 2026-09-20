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
package io.trino.plugin.hudi.util;

import io.trino.plugin.hudi.HudiErrorCode;
import io.trino.spi.TrinoException;
import org.apache.hudi.common.model.HoodieTableType;

import static io.trino.hive.formats.HiveClassNames.HUDI_INPUT_FORMAT;
import static io.trino.hive.formats.HiveClassNames.HUDI_PARQUET_INPUT_FORMAT;
import static io.trino.hive.formats.HiveClassNames.HUDI_PARQUET_REALTIME_INPUT_FORMAT;
import static io.trino.hive.formats.HiveClassNames.HUDI_REALTIME_INPUT_FORMAT;

/**
 * Decodes the Hudi table type carried by a Hive Metastore input format.
 * <p>
 * The input format is the only place the table type is recorded in the metastore, and
 * {@code HiveUtil#isHudiTable} recognises a Hudi table by that field alone. A table registered with
 * an input format {@link #fromInputFormat} cannot read is therefore a table the connector refuses
 * to query. New metastore entries get their format from the shared
 * {@code HoodieMetastoreTableDescriptor}; this class retains the broader legacy read mapping.
 */
public class HudiTableTypeUtils
{
    private HudiTableTypeUtils()
    {
    }

    public static HoodieTableType fromInputFormat(String inputFormat)
    {
        switch (inputFormat) {
            case HUDI_PARQUET_INPUT_FORMAT:
            case HUDI_INPUT_FORMAT:
                return HoodieTableType.COPY_ON_WRITE;
            case HUDI_PARQUET_REALTIME_INPUT_FORMAT:
            case HUDI_REALTIME_INPUT_FORMAT:
                return HoodieTableType.MERGE_ON_READ;
            default:
                throw new TrinoException(HudiErrorCode.HUDI_UNSUPPORTED_TABLE_TYPE, "Table has an unsupported input format: " + inputFormat);
        }
    }
}
