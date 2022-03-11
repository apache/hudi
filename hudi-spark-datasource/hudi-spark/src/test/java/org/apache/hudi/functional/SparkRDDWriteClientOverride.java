/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.functional;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.config.HoodieWriteConfig;

// Sole purpose of this class is to provide access to otherwise API inaccessible from the tests.
// While it's certainly not a great pattern, it would require substantial test restructuring to
// eliminate such access to an internal API, so this is considered acceptable given it's very limited
// scope (w/in the current package)
class SparkRDDWriteClientOverride extends org.apache.hudi.client.SparkRDDWriteClient {

  public SparkRDDWriteClientOverride(HoodieEngineContext context, HoodieWriteConfig clientConfig) {
    super(context, clientConfig);
  }

  @Override
  public void rollbackFailedBootstrap() {
    super.rollbackFailedBootstrap();
  }
}

