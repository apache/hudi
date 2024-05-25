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

import java.io.Serializable;
import org.apache.hudi.integ.testsuite.HoodieTestSuiteWriter;
import org.apache.hudi.integ.testsuite.dag.nodes.DagNode;
import org.apache.hudi.integ.testsuite.generator.DeltaGenerator;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * This wraps the context needed for an execution of
 * a {@link DagNode#execute(ExecutionContext, int)}.
 */
public class ExecutionContext implements Serializable {

  private WriterContext writerContext;
  private transient JavaSparkContext jsc;

  public ExecutionContext(JavaSparkContext jsc, WriterContext writerContext) {
    this.writerContext = writerContext;
    this.jsc = jsc;
  }

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
