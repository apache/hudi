/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.hudi.utilities.multitable;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.HoodieCleaner;
import org.apache.hudi.utilities.UtilHelpers;

import org.apache.spark.api.java.JavaSparkContext;

class CleanTask extends TableServiceTask {

  @Override
  void run() {
    HoodieCleaner.Config cleanConfig = new HoodieCleaner.Config();
    cleanConfig.basePath = basePath;
    UtilHelpers.retry(retry, () -> {
      new HoodieCleaner(cleanConfig, props, jsc).run();
      return 0;
    }, "Clean Failed");
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    private TypedProperties props;
    private String basePath;
    private JavaSparkContext jsc;
    private int retry;

    public Builder withProps(TypedProperties props) {
      this.props = props;
      return this;
    }

    public Builder withBasePath(String basePath) {
      this.basePath = basePath;
      return this;
    }

    public Builder withJsc(JavaSparkContext jsc) {
      this.jsc = jsc;
      return this;
    }

    public Builder withRetry(int retry) {
      this.retry = retry;
      return this;
    }

    public CleanTask build() {
      CleanTask cleanTask = new CleanTask();
      cleanTask.jsc = this.jsc;
      cleanTask.retry = this.retry;
      cleanTask.basePath = this.basePath;
      cleanTask.props = this.props;
      return cleanTask;
    }
  }
}
