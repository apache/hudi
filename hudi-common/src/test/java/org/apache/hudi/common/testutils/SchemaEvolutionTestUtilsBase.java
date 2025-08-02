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

package org.apache.hudi.common.testutils;

import org.apache.hudi.avro.AvroSchemaUtils;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.AVRO_SCHEMA;

/**
 * Utils to run and validate schema evolution testing.
 * Common utils for both schema on write and schema on read for generating type promotion schemas
 */
public class SchemaEvolutionTestUtilsBase {

  // Sequence of events and validations to capture all the scenarios that a file slice
  // can be written with regard to the table schema
  public enum SchemaEvolutionScenarioType {
    // base file written with schema A, read with schema B
    BASE_FILES_WITH_DIFFERENT_SCHEMA(new SchemaEvolutionScenario()
        .writeData(WriteDataConfig.baseFile(5, "001", "any_partition"))
        .validate(0)
        .evolveSchema()
        .writeData(WriteDataConfig.baseFile(5, "002", "new_partition"))
        .validate(0)),

    // file slice with base file written with schema A, log file written with schema A, read with schema B
    BASE_FILE_HAS_DIFFERENT_SCHEMA_THAN_LOG_FILES(new SchemaEvolutionScenario()
        .writeData(WriteDataConfig.baseFile(10, "001", "any_partition"))
        .validate(0)
        .writeData(WriteDataConfig.logFile(5, "002"))
        .validate(1)
        .evolveSchema()
        .writeData(WriteDataConfig.baseFile(5, "003", "new_partition"))
        .validate(-1)),

    // file slice with base file written with schema A, log file written with schema A, and log file written with schema B
    // read with schema B
    LOG_FILES_WITH_DIFFERENT_SCHEMA(new SchemaEvolutionScenario()
        .writeData(WriteDataConfig.baseFile(100, "001"))
        .validate(0)
        .writeData(WriteDataConfig.logFile(50, "002"))
        .validate(1)
        .evolveSchema()
        .writeData(WriteDataConfig.logFile(50, "003"))
        .validate(2)),

    // file slice with base file written with schema A, log file written with schema A, and log file written with schema B
    // read with schema C
    LOG_FILES_WITH_DIFFERENT_SCHEMA_AND_TABLE_SCHEMA_DIFFERS(new SchemaEvolutionScenario()
        .writeData(WriteDataConfig.baseFile(100, "001"))
        .validate(0)
        .writeData(WriteDataConfig.logFile(50, "002"))
        .validate(1)
        .evolveSchema()
        .writeData(WriteDataConfig.logFile(50, "003"))
        .validate(2)
        .evolveSchema()
        .writeData(WriteDataConfig.baseFile(5, "004", "new_partition"))
        .validate(-1)),

    // file slice with base file written with schema A, log file written with schema B, read with schema B
    BASE_FILES_WITH_DIFFERENT_SCHEMA_FROM_LOG_FILES(new SchemaEvolutionScenario()
        .writeData(WriteDataConfig.baseFile(100, "001"))
        .validate(0)
        .evolveSchema()
        .writeData(WriteDataConfig.logFile(50, "002"))
        .validate(1));

    private final SchemaEvolutionScenario scenario;
    SchemaEvolutionScenarioType(SchemaEvolutionScenario scenario) {
      this.scenario = scenario;
    }

    public SchemaEvolutionScenario getScenario() {
      return scenario;
    }
  }

  public static final class SchemaEvolutionScenario {
    private int stepIndex = 0;
    private int currentIteration = 0;
    private final List<TestStep> steps = new ArrayList<>();

    private List<TestStep> getSteps() {
      return steps;
    }

    SchemaEvolutionScenario writeData(WriteDataConfig dataConfig) {
      steps.add(new WriteDataStep(stepIndex++, dataConfig));
      return this;
    }

    SchemaEvolutionScenario evolveSchema() {
      steps.add(new EvolveSchemaStep(stepIndex++, ++currentIteration));
      return this;
    }

    SchemaEvolutionScenario validate(int expectedLogFiles) {
      steps.add(new ValidateStep(stepIndex++, expectedLogFiles));
      return this;
    }

    public int getMaxIterations() {
      return currentIteration;
    }

  }

  // Write can be a base file or a log file, so we use
  // this class to differentiate
  public static class WriteDataConfig {
    public final int recordCount;
    public final String commitId;
    public final String partition;
    public final boolean isBaseFile;

    WriteDataConfig(int recordCount, String commitId, String partition, boolean isBaseFile) {
      this.recordCount = recordCount;
      this.commitId = commitId;
      this.partition = partition;
      this.isBaseFile = isBaseFile;
    }

    static WriteDataConfig baseFile(int recordCount, String commitId) {
      return baseFile(recordCount, commitId, null);
    }

    static WriteDataConfig baseFile(int recordCount, String commitId, String partition) {
      return new WriteDataConfig(recordCount, commitId, partition, true);
    }

    static WriteDataConfig logFile(int recordCount, String commitId) {
      return new WriteDataConfig(recordCount, commitId, null, false);
    }
  }

  // Represents a single test step during a schema evolution test
  private interface TestStep {
    void execute(SchemaEvolutionTestExecutor executor) throws Exception;
  }

  // Represents a step that writes data
  private static class WriteDataStep implements TestStep {
    private final WriteDataConfig config;
    private final int stepIndex;

    WriteDataStep(int stepIndex, WriteDataConfig config) {
      this.config = config;
      this.stepIndex = stepIndex;
    }

    @Override
    public void execute(SchemaEvolutionTestExecutor executor) throws Exception {
      executor.writeData(config);
    }
  }

  // Represents a step that evolves the schema
  private static class EvolveSchemaStep implements TestStep {
    private final int iteration;
    private final int stepIndex;

    EvolveSchemaStep(int stepIndex, int iteration) {
      this.iteration = iteration;
      this.stepIndex = stepIndex;
    }

    @Override
    public void execute(SchemaEvolutionTestExecutor executor) throws Exception {
      executor.evolveSchema(iteration);
    }
  }

  // Represents a step that validates the data
  private static class ValidateStep implements TestStep {
    private final int expectedLogFiles;
    private final int stepIndex;

    ValidateStep(int stepIndex, int expectedLogFiles) {
      this.stepIndex = stepIndex;
      this.expectedLogFiles = expectedLogFiles;
    }

    @Override
    public void execute(SchemaEvolutionTestExecutor executor) throws Exception {
      executor.validate(expectedLogFiles);
    }
  }

  public static void executeTest(SchemaEvolutionTestExecutor executor, SchemaEvolutionScenario scenario) throws Exception {
    for (TestStep step : scenario.getSteps()) {
      step.execute(executor);
    }
  }

  // Interface for test to implement to execute a schema evolution test
  public interface SchemaEvolutionTestExecutor extends AutoCloseable {
    void writeData(WriteDataConfig dataConfig) throws Exception;

    void evolveSchema(int iteration) throws Exception;

    void validate(int expectedLogFiles) throws Exception;
  }

  // Used to represent a field to be added for type promotion testing
  public static class SchemaEvolutionField {
    public SchemaEvolutionField(String name, Schema.Type type, LogicalType logicalType) {
      this.name = name;
      this.type = type;
      this.logicalType = logicalType;
    }

    public String name;
    public Schema.Type type;
    public LogicalType logicalType;
  }

  // Configuration for schema evolution type promotion schema generation
  public static class SchemaEvolutionConfigBase {
    public Schema schema = AVRO_SCHEMA;
    public boolean nestedSupport = true;
    public boolean mapSupport = true;
    public boolean arraySupport = true;
    // TODO: [HUDI-9603] Flink 1.18 array values incorrect in fg reader test
    public boolean anyArraySupport = true;

    // Int
    public boolean intToLongSupport = true;
    public boolean intToFloatSupport = true;
    public boolean intToDoubleSupport = true;
    public boolean intToStringSupport = true;

    // Long
    public boolean longToFloatSupport = true;
    public boolean longToDoubleSupport = true;
    public boolean longToStringSupport = true;

    // Float
    public boolean floatToDoubleSupport = true;
    public boolean floatToStringSupport = true;

    // Double
    public boolean doubleToStringSupport = true;
  }

  // Utility method to extend configs.schema with fields specified in baseFields
  protected static Schema generateExtendedSchema(SchemaEvolutionConfigBase configs, List<SchemaEvolutionField> baseFields) {
    return generateExtendedSchema(configs.schema, configs, baseFields, "customField", true);
  }

  private static Schema generateExtendedSchema(Schema baseSchema, SchemaEvolutionConfigBase configs, List<SchemaEvolutionField> baseFields, String fieldPrefix, boolean toplevel) {
    List<Schema.Field> fields =  baseSchema.getFields();
    List<Schema.Field> finalFields = new ArrayList<>(fields.size() + baseFields.size());
    boolean addedFields = false;
    for (Schema.Field field : fields) {
      if (configs.nestedSupport && field.name().equals("fare") && field.schema().getType() == Schema.Type.RECORD) {
        finalFields.add(new Schema.Field(field.name(), generateExtendedSchema(field.schema(), configs, baseFields, "customFare", false), field.doc(), field.defaultVal()));
      } else if (configs.anyArraySupport || !field.name().equals("tip_history")) {
        //TODO: [HUDI-9603] remove the if condition when the issue is fixed
        if (field.name().equals("_hoodie_is_deleted")) {
          addedFields = true;
          addFields(configs, finalFields, baseFields, fieldPrefix, baseSchema.getNamespace(), toplevel);
        }
        finalFields.add(new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal()));
      }
    }
    if (!addedFields) {
      addFields(configs, finalFields, baseFields, fieldPrefix, baseSchema.getNamespace(), toplevel);
    }
    Schema finalSchema = Schema.createRecord(baseSchema.getName(), baseSchema.getDoc(),
        baseSchema.getNamespace(), baseSchema.isError());
    finalSchema.setFields(finalFields);
    return finalSchema;
  }

  private static void addFields(SchemaEvolutionConfigBase configs, List<Schema.Field> finalFields,
                                List<SchemaEvolutionField> baseFields, String fieldPrefix,
                                String namespace, boolean toplevel) {
    if (toplevel) {
      if (configs.mapSupport) {
        List<Schema.Field> mapFields = new ArrayList<>(baseFields.size());
        addFieldsHelper(mapFields, baseFields, fieldPrefix + "Map");
        finalFields.add(new Schema.Field(fieldPrefix + "Map", Schema.createMap(Schema.createRecord("customMapRecord", "", namespace, false, mapFields)), "", null));
      }

      if (configs.arraySupport) {
        List<Schema.Field> arrayFields = new ArrayList<>(baseFields.size());
        addFieldsHelper(arrayFields, baseFields, fieldPrefix + "Array");
        finalFields.add(new Schema.Field(fieldPrefix + "Array", Schema.createArray(Schema.createRecord("customArrayRecord", "", namespace, false, arrayFields)), "", null));
      }
    }
    addFieldsHelper(finalFields, baseFields, fieldPrefix);
  }

  private static void addFieldsHelper(List<Schema.Field> finalFields, List<SchemaEvolutionField> baseFields, String fieldPrefix) {
    for (int i = 0; i < baseFields.size(); i++) {
      if (baseFields.get(i).logicalType == null) {
        if (baseFields.get(i).type == Schema.Type.BOOLEAN) {
          // boolean fields are added fields
          finalFields.add(new Schema.Field(fieldPrefix + baseFields.get(i).name + i, AvroSchemaUtils.createNullableSchema(Schema.Type.BOOLEAN), "", null));
        } else {
          finalFields.add(new Schema.Field(fieldPrefix + baseFields.get(i).name + i, Schema.create(baseFields.get(i).type), "", null));
        }
      } else {
        if (baseFields.get(i).logicalType instanceof LogicalTypes.Decimal) {
          LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) baseFields.get(i).logicalType;
          Schema fieldSchema;
          if (baseFields.get(i).type == Schema.Type.FIXED) {
            fieldSchema = Schema.createFixed(fieldPrefix + baseFields.get(i).name + i, "", "", 30);
          } else {
            fieldSchema = Schema.create(baseFields.get(i).type);
          }

          finalFields.add(new Schema.Field(fieldPrefix + baseFields.get(i).name + i, LogicalTypes.decimal(decimal.getPrecision(), decimal.getScale()).addToSchema(fieldSchema), "", null));
        } else if (baseFields.get(i).logicalType instanceof LogicalTypes.Date) {
          Schema fieldSchema = LogicalTypes.date().addToSchema(Schema.create(baseFields.get(i).type));
          finalFields.add(new Schema.Field(fieldPrefix + baseFields.get(i).name + i, fieldSchema, "", null));
        } else {
          throw new IllegalStateException("Unsupported logical type: " + baseFields.get(i).logicalType);
        }
      }
    }
  }
}
