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
import org.apache.hudi.common.model.WriteOperationType;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hudi.common.model.WriteOperationType.INSERT;
import static org.apache.hudi.common.model.WriteOperationType.UPSERT;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.AVRO_SCHEMA;

class SchemaEvolutionTestUtilsBase {

  // Test data configuration
  protected static class WriteDataConfig {
    final int recordCount;
    final String commitId;
    final String partition;
    final WriteOperationType operation;
    final boolean isBaseFile;

    WriteDataConfig(int recordCount, String commitId, String partition,
                   WriteOperationType operation, boolean isBaseFile) {
      this.recordCount = recordCount;
      this.commitId = commitId;
      this.partition = partition;
      this.operation = operation;
      this.isBaseFile = isBaseFile;
    }

    static WriteDataConfig baseFile(int recordCount, String commitId) {
      return baseFile(recordCount, commitId, null);
    }

    static WriteDataConfig baseFile(int recordCount, String commitId, String partition) {
      return new WriteDataConfig(recordCount, commitId, partition, INSERT, true);
    }

    static WriteDataConfig logFile(int recordCount, String commitId) {
      return new WriteDataConfig(recordCount, commitId, null, UPSERT, false);
    }
  }


  public enum SchemaEvolutionScenarioType {
    BASE_FILES_WITH_DIFFERENT_SCHEMA(new SchemaEvolutionScenario(1)
        .writeData(WriteDataConfig.baseFile(5, "001", "any_partition"))
        .validate(0)
        .evolveSchema()
        .writeData(WriteDataConfig.baseFile(5, "002", "new_partition"))
        .validate(0)),
    BASE_FILE_HAS_DIFFERENT_SCHEMA_THAN_LOG_FILES(new SchemaEvolutionScenario(1)
        .writeData(WriteDataConfig.baseFile(10, "001", "any_partition"))
        .validate(0)
        .writeData(WriteDataConfig.logFile(5, "002"))
        .validate(1)
        .evolveSchema()
        .writeData(WriteDataConfig.baseFile(5, "003", "new_partition"))
        .validate(-1)),
    LOG_FILES_WITH_DIFFERENT_SCHEMA(new SchemaEvolutionScenario(1)
        .writeData(WriteDataConfig.baseFile(100, "001"))
        .validate(0)
        .writeData(WriteDataConfig.logFile(50, "002"))
        .validate(1)
        .evolveSchema()
        .writeData(WriteDataConfig.logFile(50, "003"))
        .validate(2)),

    LOG_FILES_WITH_DIFFERENT_SCHEMA_AND_TABLE_SCHEMA_DIFFERS(new SchemaEvolutionScenario(2)
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
    BASE_FILES_WITH_DIFFERENT_SCHEMA_FROM_LOG_FILES(new SchemaEvolutionScenario(1)
        .writeData(WriteDataConfig.baseFile(100, "001"))
        .validate(0)
        .evolveSchema()
        .writeData(WriteDataConfig.logFile(50, "002"))
        .validate(1));

    private final SchemaEvolutionScenario scenario;
    SchemaEvolutionScenarioType(SchemaEvolutionScenario scenario) {
      this.scenario = scenario;
    }
  }

  public static final class SchemaEvolutionScenario {
    private int currentIteration = 0;
    private final int maxIterations;
    private final List<TestStep> steps = new ArrayList<>();

    private List<TestStep> getSteps() {
      return steps;
    }

    public SchemaEvolutionScenario(int maxIterations) {
      this.maxIterations = maxIterations;
    }

    SchemaEvolutionScenario writeData(WriteDataConfig dataConfig) {
      steps.add(new WriteDataStep(dataConfig));
      return this;
    }

    SchemaEvolutionScenario evolveSchema() {
      steps.add(new EvolveSchemaStep(currentIteration++, maxIterations));
      return this;
    }

    SchemaEvolutionScenario validate(int expectedLogFiles) {
      steps.add(new ValidateStep(expectedLogFiles));
      return this;
    }

  }

  // Abstract test step
  private interface TestStep {
    void execute(SchemaEvolutionTestExecutor executor) throws Exception;
  }

  // Concrete test steps
  private static class WriteDataStep implements TestStep {
    private final WriteDataConfig config;

    WriteDataStep(WriteDataConfig config) {
      this.config = config;
    }

    @Override
    public void execute(SchemaEvolutionTestExecutor executor) throws Exception {
      executor.writeData(config);
    }
  }

  private static class EvolveSchemaStep implements TestStep {
    private final int iteration;
    private final int maxIterations;

    EvolveSchemaStep(int iteration, int maxIterations) {
      this.iteration = iteration;
      this.maxIterations = maxIterations;
    }

    @Override
    public void execute(SchemaEvolutionTestExecutor executor) throws Exception {
      executor.evolveSchema(iteration, maxIterations);
    }
  }

  private static class ValidateStep implements TestStep {
    private final int expectedLogFiles;

    ValidateStep(int expectedLogFiles) {
      this.expectedLogFiles = expectedLogFiles;
    }

    @Override
    public void execute(SchemaEvolutionTestExecutor executor) throws Exception {
      executor.validate(expectedLogFiles);
    }
  }

  public interface SchemaEvolutionTestExecutor extends AutoCloseable {

    void writeData(WriteDataConfig dataConfig) throws Exception;

    void evolveSchema(int iteration, int maxIterations) throws Exception;

    void validate(int expectedLogFiles) throws Exception;

    default void execute(SchemaEvolutionScenario scenario) throws Exception {
      for (TestStep step : scenario.getSteps()) {
        step.execute(this);
      }
    }
  }

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

  public static class SchemaEvolutionConfigBase {
    public Schema schema = AVRO_SCHEMA;
    public boolean nestedSupport = true;
    public boolean mapSupport = true;
    public boolean arraySupport = true;
    // TODO: [HUDI-9603] Flink 1.18 array values incorrect in fg reader test
    public boolean anyArraySupport = true;
  }

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
          finalFields.add(new Schema.Field(fieldPrefix + baseFields.get(i).name, AvroSchemaUtils.createNullableSchema(Schema.Type.BOOLEAN), "", null));
        } else {
          finalFields.add(new Schema.Field(fieldPrefix + baseFields.get(i).name, Schema.create(baseFields.get(i).type), "", null));
        }
      } else {
        if (baseFields.get(i).logicalType instanceof LogicalTypes.Decimal) {
          LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) baseFields.get(i).logicalType;
          Schema fieldSchema;
          if (baseFields.get(i).type == Schema.Type.FIXED) {
            fieldSchema = Schema.createFixed(fieldPrefix + baseFields.get(i).name, "", "", 30);
          } else {
            fieldSchema = Schema.create(baseFields.get(i).type);
          }

          finalFields.add(new Schema.Field(fieldPrefix + baseFields.get(i).name, LogicalTypes.decimal(decimal.getPrecision(), decimal.getScale()).addToSchema(fieldSchema), "", null));
        } else if (baseFields.get(i).logicalType instanceof LogicalTypes.Date) {
          Schema fieldSchema = LogicalTypes.date().addToSchema(Schema.create(baseFields.get(i).type));
          finalFields.add(new Schema.Field(fieldPrefix + baseFields.get(i).name, fieldSchema, "", null));
        } else {
          throw new IllegalStateException("Unsupported logical type: " + baseFields.get(i).logicalType);
        }
      }
    }
  }
}
