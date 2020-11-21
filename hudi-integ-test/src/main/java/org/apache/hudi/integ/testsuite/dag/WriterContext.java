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

import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.integ.testsuite.HoodieTestSuiteJob.HoodieTestSuiteConfig;
import org.apache.hudi.integ.testsuite.HoodieTestSuiteWriter;
import org.apache.hudi.integ.testsuite.configuration.DFSDeltaConfig;
import org.apache.hudi.integ.testsuite.generator.DeltaGenerator;
import org.apache.hudi.integ.testsuite.reader.DeltaInputType;
import org.apache.hudi.integ.testsuite.writer.DeltaOutputMode;
import org.apache.hudi.keygen.BuiltinKeyGenerator;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/**
 * WriterContext wraps the delta writer/data generator related configuration needed to init/reinit.
 */
public class WriterContext {

  protected static Logger log = LogManager.getLogger(WriterContext.class);

  private final HoodieTestSuiteConfig cfg;
  private TypedProperties props;
  private HoodieTestSuiteWriter hoodieTestSuiteWriter;
  private DeltaGenerator deltaGenerator;
  private transient SchemaProvider schemaProvider;
  private BuiltinKeyGenerator keyGenerator;
  private transient SparkSession sparkSession;
  private transient JavaSparkContext jsc;

  public WriterContext(JavaSparkContext jsc, TypedProperties props, HoodieTestSuiteConfig cfg,
      BuiltinKeyGenerator keyGenerator, SparkSession sparkSession) {
    this.cfg = cfg;
    this.props = props;
    this.keyGenerator = keyGenerator;
    this.sparkSession = sparkSession;
    this.jsc = jsc;
  }

  public void initContext(JavaSparkContext jsc) throws HoodieException {
    try {
      this.schemaProvider = UtilHelpers.createSchemaProvider(cfg.schemaProviderClassName, props, jsc);
      String schemaStr = schemaProvider.getSourceSchema().toString();
      this.hoodieTestSuiteWriter = new HoodieTestSuiteWriter(jsc, props, cfg, schemaStr);
      int inputParallelism = cfg.inputParallelism > 0 ? cfg.inputParallelism : jsc.defaultParallelism();
      this.deltaGenerator = new DeltaGenerator(
          new DFSDeltaConfig(DeltaOutputMode.valueOf(cfg.outputTypeName), DeltaInputType.valueOf(cfg.inputFormatName),
              new SerializableConfiguration(jsc.hadoopConfiguration()), cfg.inputBasePath, cfg.targetBasePath,
              schemaStr, cfg.limitFileSize, inputParallelism, cfg.deleteOldInput),
          jsc, sparkSession, schemaStr, keyGenerator);
      log.info(String.format("Initialized writerContext with: %s", schemaStr));
    } catch (Exception e) {
      throw new HoodieException("Failed to reinitialize writerContext", e);
    }
  }

  public void reinitContext(Map<String, Object> newConfig) throws HoodieException {
    // update props with any config overrides.
    for (Map.Entry<String, Object> e : newConfig.entrySet()) {
      if (this.props.containsKey(e.getKey())) {
        this.props.setProperty(e.getKey(), e.getValue().toString());
      }
    }
    initContext(jsc);
  }

  public HoodieTestSuiteWriter getHoodieTestSuiteWriter() {
    return hoodieTestSuiteWriter;
  }

  public DeltaGenerator getDeltaGenerator() {
    return deltaGenerator;
  }

  public HoodieTestSuiteConfig getCfg() {
    return cfg;
  }

  public TypedProperties getProps() {
    return props;
  }

  public String toString() {
    return this.hoodieTestSuiteWriter.toString() + "\n" + this.deltaGenerator.toString() + "\n";
  }
}
