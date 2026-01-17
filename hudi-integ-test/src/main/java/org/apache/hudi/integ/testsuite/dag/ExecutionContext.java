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

package org.apache.hudi.integ.testsuite.dag;

import org.apache.hudi.integ.testsuite.HoodieTestSuiteWriter;
import org.apache.hudi.integ.testsuite.dag.nodes.DagNode;
import org.apache.hudi.integ.testsuite.generator.DeltaGenerator;

import lombok.AllArgsConstructor;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

/**
 * This wraps the context needed for an execution of
 * a {@link DagNode#execute(ExecutionContext, int)}.
 *
 * Note: Getters are manually defined instead of using Lombok's @Getter annotation.
 * This is because Scala classes in this module reference these getters, and the Scala
 * compiler runs before Lombok annotation processing. Using @Getter would cause Scala
 * compilation errors as the generated methods wouldn't be visible yet. To avoid
 * complicating the build with custom compilation order configuration, we use explicit
 * getter methods that are available during Scala compilation.
 */
@AllArgsConstructor
public class ExecutionContext implements Serializable {

  private transient JavaSparkContext jsc;
  private WriterContext writerContext;

  public HoodieTestSuiteWriter getHoodieTestSuiteWriter() {
    return writerContext.getHoodieTestSuiteWriter();
  }

  public DeltaGenerator getDeltaGenerator() {
    return writerContext.getDeltaGenerator();
  }

  public JavaSparkContext getJsc() {
    return jsc;
  }

  public WriterContext getWriterContext() {
    return writerContext;
  }
}
