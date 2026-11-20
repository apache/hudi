/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities.unstructured.embedding.image;

import java.io.IOException;

/** The default {@link ImageDecoder}: runs {@link ImageResizeLogicV1} in the calling thread of this JVM. */
public class InProcessImageDecoder implements ImageDecoder {

  @Override
  public String logicVersion() {
    return ImageResizeLogicV1.LOGIC_VERSION;
  }

  @Override
  public PreparedImage decode(ImageSource source, RasterPlan plan, ImagePreprocessingSpec spec) throws IOException {
    return ImageResizeLogicV1.run(source, plan, spec);
  }
}
