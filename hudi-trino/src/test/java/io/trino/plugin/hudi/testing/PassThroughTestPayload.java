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

import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.util.Option;

/**
 * {@link OverwriteWithLatestAvroPayload} under a class name of its own. Hudi translates the payloads it is deprecating,
 * {@code OverwriteWithLatestAvroPayload} among them, into a merge mode when the table is created, so the read never
 * builds a payload; a payload outside that set keeps the CUSTOM merge mode, and the read merges through the payload,
 * serializing the base record through {@code BaseAvroPayload}.
 */
public class PassThroughTestPayload
        extends OverwriteWithLatestAvroPayload
{
    public PassThroughTestPayload(GenericRecord record, Comparable orderingVal)
    {
        super(record, orderingVal);
    }

    public PassThroughTestPayload(Option<GenericRecord> record)
    {
        super(record);
    }
}
