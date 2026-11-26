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

package org.apache.hudi.cli;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Tests how {@link BootstrapExecutorUtils} applies the configured CLI {@link SchemaProvider} to the bootstrap
 * write config. The constructor runs no Spark job, so a mocked {@link JavaSparkContext} is enough to load the
 * provider through its (TypedProperties, JavaSparkContext) constructor.
 */
public class TestBootstrapExecutorUtils {

  // The executor only builds the write config, so the paths are never touched.
  private static final String BASE_PATH = "/tmp/hudi/bootstrap_schema_test";
  private static final Schema SOURCE_SCHEMA = new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"s\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}");
  private static final Schema TARGET_SCHEMA = new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"t\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},"
          + "{\"name\":\"ts\",\"type\":\"long\",\"default\":0}]}");
  private static final String PROPS_SCHEMA =
      "{\"type\":\"record\",\"name\":\"p\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}";

  @Test
  void testWriteSchemaFromProviderImplementingOnlyHoodieSchemaGetter() throws IOException {
    BootstrapExecutorUtils executor = createExecutor(HoodieSchemaOnlyProvider.class);

    assertEquals(SOURCE_SCHEMA.toString(), executor.getBootstrapConfig().getSchema());
  }

  @Test
  void testWriteSchemaIsLegacyProviderTargetSchema() throws IOException {
    BootstrapExecutorUtils executor = createExecutor(LegacyAvroProvider.class);

    assertEquals(TARGET_SCHEMA.toString(), executor.getBootstrapConfig().getSchema());
  }

  @Test
  void testWriteSchemaIsHoodieSchemaTargetOverride() throws IOException {
    BootstrapExecutorUtils executor = createExecutor(HoodieSchemaTargetProvider.class);

    assertEquals(TARGET_SCHEMA.toString(), executor.getBootstrapConfig().getSchema());
  }

  /**
   * A null target keeps the configured write schema, whether it comes from the source fallback
   * ({@link NullSchemaProvider}) or from a HoodieSchema target override that ignores a non-null legacy source
   * schema ({@link NullHoodieSchemaTargetProvider}).
   */
  @ParameterizedTest
  @ValueSource(classes = {NullSchemaProvider.class, NullHoodieSchemaTargetProvider.class})
  void testNullTargetSchemaKeepsConfiguredWriteSchema(Class<? extends SchemaProvider> providerClass) throws IOException {
    BootstrapExecutorUtils executor = createExecutor(providerClass);

    assertEquals(PROPS_SCHEMA, executor.getBootstrapConfig().getSchema());
  }

  private BootstrapExecutorUtils createExecutor(Class<? extends SchemaProvider> schemaProviderClass) throws IOException {
    BootstrapExecutorUtils.Config cfg = new BootstrapExecutorUtils.Config();
    cfg.setBasePath(BASE_PATH);
    cfg.setTableName("bootstrap_schema_test");
    cfg.setPayloadClass(OverwriteWithLatestAvroPayload.class.getName());
    cfg.setSchemaProviderClass(schemaProviderClass.getName());

    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieTableConfig.BOOTSTRAP_BASE_PATH.key(), BASE_PATH + "_source");
    props.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "id");
    props.setProperty(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), NonpartitionedKeyGenerator.class.getName());
    props.setProperty(HoodieWriteConfig.AVRO_SCHEMA_STRING.key(), PROPS_SCHEMA);
    // ReflectionUtils.loadClass matches the provider constructor on the arguments' runtime classes; Mockito's
    // default inline mock maker keeps mock(JavaSparkContext.class).getClass() == JavaSparkContext.class.
    return new BootstrapExecutorUtils(cfg, mock(JavaSparkContext.class), null, new Configuration(), props);
  }

  /**
   * Implements only the HoodieSchema getter; the deprecated Avro getters keep their throwing defaults.
   */
  public static class HoodieSchemaOnlyProvider extends SchemaProvider {
    public HoodieSchemaOnlyProvider(TypedProperties props, JavaSparkContext jssc) {
      super(props, jssc);
    }

    @Override
    public HoodieSchema getSourceHoodieSchema() {
      return HoodieSchema.fromAvroSchema(SOURCE_SCHEMA);
    }
  }

  /**
   * Implements only the deprecated Avro getters, with a target schema distinct from the source one.
   */
  public static class LegacyAvroProvider extends SchemaProvider {
    public LegacyAvroProvider(TypedProperties props, JavaSparkContext jssc) {
      super(props, jssc);
    }

    @Override
    @Deprecated
    public Schema getSourceSchema() {
      return SOURCE_SCHEMA;
    }

    @Override
    @Deprecated
    public Schema getTargetSchema() {
      return TARGET_SCHEMA;
    }
  }

  /**
   * Supplies no schema, so the bootstrap write config keeps the configured one.
   */
  public static class NullSchemaProvider extends SchemaProvider {
    public NullSchemaProvider(TypedProperties props, JavaSparkContext jssc) {
      super(props, jssc);
    }

    @Override
    public HoodieSchema getSourceHoodieSchema() {
      return null;
    }
  }

  /**
   * Implements only the HoodieSchema getters, with a target schema distinct from the source one.
   */
  public static class HoodieSchemaTargetProvider extends HoodieSchemaOnlyProvider {
    public HoodieSchemaTargetProvider(TypedProperties props, JavaSparkContext jssc) {
      super(props, jssc);
    }

    @Override
    public HoodieSchema getTargetHoodieSchema() {
      return HoodieSchema.fromAvroSchema(TARGET_SCHEMA);
    }
  }

  /**
   * Implements the deprecated Avro source getter but supplies no target through the HoodieSchema getter.
   */
  public static class NullHoodieSchemaTargetProvider extends SchemaProvider {
    public NullHoodieSchemaTargetProvider(TypedProperties props, JavaSparkContext jssc) {
      super(props, jssc);
    }

    @Override
    @Deprecated
    public Schema getSourceSchema() {
      return SOURCE_SCHEMA;
    }

    @Override
    public HoodieSchema getTargetHoodieSchema() {
      return null;
    }
  }
}
