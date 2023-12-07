/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.io.hfile;

import org.apache.hudi.io.hfile.compress.Compression;

/**
 * The context of HFile that contains information of the blocks.
 */
public class HFileContext {
  private Compression.Algorithm compressAlgo;

  private HFileContext(Compression.Algorithm compressAlgo) {
    this.compressAlgo = compressAlgo;
  }

  Compression.Algorithm getCompressAlgo() {
    return compressAlgo;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private Compression.Algorithm compressAlgo = Compression.Algorithm.NONE;

    public Builder() {
    }

    public Builder compressAlgo(Compression.Algorithm compressAlgo) {
      this.compressAlgo = compressAlgo;
      return this;
    }

    public HFileContext build() {
      return new HFileContext(compressAlgo);
    }
  }
}
