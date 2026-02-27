/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.lookup;

import org.apache.flink.table.connector.source.LookupTableSource.LookupRuntimeProvider;
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;

/** Factory to create {@link LookupRuntimeProvider}. */
public class LookupRuntimeProviderFactory {

  public static LookupRuntimeProvider create(HoodieLookupFunction function, boolean enableAsync, int asyncThreadNumber) {
    return enableAsync
        ? AsyncLookupFunctionProvider.of(new AsyncLookupFunctionWrapper(function, asyncThreadNumber))
        : LookupFunctionProvider.of(function);
  }
}
