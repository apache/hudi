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

import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.internal.schema.action.TableChange;
import org.apache.hudi.internal.schema.action.TableChanges;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.apache.hudi.internal.schema.utils.SchemaChangeUtils;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

public class SchemaOnReadEvolutionTestUtils extends SchemaEvolutionTestUtilsBase {

  public static InternalSchema extendSchema(SchemaOnReadConfigs configs, int iteration) {
    validateConfigs(configs);

    List<Pair<Schema.Type, LogicalType>> baseFields = buildBaseFieldsList(configs, iteration);
    Schema tmpExtendedSchema = generateExtendedSchema(configs, baseFields);
    InternalSchema modificationSchema = AvroInternalSchemaConverter.convert(tmpExtendedSchema);

    SchemaModifier modifier = new SchemaModifier(configs);

    // Apply initial schema modifications
    modificationSchema = addInitialFields(modificationSchema, configs, modifier);

    // Apply iterative modifications
    modificationSchema = applyIterativeModifications(modificationSchema, configs, modifier, iteration);

    return modificationSchema;
  }

  public static class SchemaOnReadConfigs extends SchemaEvolutionTestUtilsBase.SchemaEvolutionConfigBase {
    public int numRoundsSupported = 5;
    public boolean addNewFieldSupport = true;
    public boolean addNewFieldNotAtEndSupport = true;
    public boolean reorderColumnSupport = true;
    public boolean renameColumnSupport = true;
    public boolean removeColumnSupport = true;
    public boolean renameColumnAsPreviouslyRemovedSupport = true;

    // Int
    public boolean intToLongSupport = true;
    public boolean intToFloatSupport = true;
    public boolean intToDoubleSupport = true;
    public boolean intToStringSupport = true;
    public boolean intToDecimalFixedSupport = true;
    public boolean intToDecimalBytesSupport = true;

    // Long
    public boolean longToFloatSupport = true;
    public boolean longToDoubleSupport = true;
    public boolean longToStringSupport = true;
    public boolean longToDecimalFixedSupport = true;
    public boolean longToDecimalBytesSupport = true;

    // Float
    public boolean floatToDoubleSupport = true;
    public boolean floatToStringSupport = true;
    public boolean floatToDecimalFixedSupport = true;
    public boolean floatToDecimalBytesSupport = true;

    // Double
    public boolean doubleToStringSupport = true;
    public boolean doubleToDecimalFixedSupport = true;
    public boolean doubleToDecimalBytesSupport = true;

    // String
    public boolean stringToDecimalFixedSupport = true;
    public boolean stringToDecimalBytesSupport = true;
    public boolean stringToDateSupport = true;

    // Decimal
    public boolean decimalFixedToStringSupport = true;
    public boolean decimalBytesToStringSupport = true;

    // Date
    public boolean dateToStringSupport = true;
  }

  //TODO: decide if logical types should have more cases
  // also if we need to test evolution where the main type is the same but the logical type changes
  private enum SchemaOnReadTypePromotionCase {
    INT_TO_INT(Schema.Type.INT, null, Schema.Type.INT, null, config -> true),
    INT_TO_LONG(Schema.Type.INT, null, Schema.Type.LONG, null, config -> config.intToLongSupport),
    INT_TO_FLOAT(Schema.Type.INT, null, Schema.Type.FLOAT, null, config -> config.intToFloatSupport),
    INT_TO_DOUBLE(Schema.Type.INT, null, Schema.Type.DOUBLE, null, config -> config.intToDoubleSupport),
    INT_TO_STRING(Schema.Type.INT, null, Schema.Type.STRING, null, config -> config.intToStringSupport),
    INT_TO_DECIMAL_FIXED(Schema.Type.INT, null, Schema.Type.FIXED, LogicalTypes.decimal(4, 3), config -> config.intToDecimalFixedSupport),
    INT_TO_DECIMAL_BYTES(Schema.Type.INT, null, Schema.Type.BYTES, LogicalTypes.decimal(4, 3), config -> config.intToDecimalBytesSupport),

    LONG_TO_LONG(Schema.Type.LONG, null, Schema.Type.LONG, null, config -> true),
    LONG_TO_FLOAT(Schema.Type.LONG, null, Schema.Type.FLOAT, null, config -> config.longToFloatSupport),
    LONG_TO_DOUBLE(Schema.Type.LONG, null, Schema.Type.DOUBLE, null, config -> config.longToDoubleSupport),
    LONG_TO_STRING(Schema.Type.LONG, null, Schema.Type.STRING, null, config -> config.longToStringSupport),
    LONG_TO_DECIMAL_FIXED(Schema.Type.LONG, null, Schema.Type.FIXED, LogicalTypes.decimal(4, 3), config -> config.longToDecimalFixedSupport),
    LONG_TO_DECIMAL_BYTES(Schema.Type.LONG, null, Schema.Type.BYTES, LogicalTypes.decimal(4, 3), config -> config.longToDecimalBytesSupport),

    FLOAT_TO_FLOAT(Schema.Type.FLOAT, null, Schema.Type.FLOAT, null, config -> true),
    FLOAT_TO_DOUBLE(Schema.Type.FLOAT, null, Schema.Type.DOUBLE, null, config -> config.floatToDoubleSupport),
    FLOAT_TO_STRING(Schema.Type.FLOAT, null, Schema.Type.STRING, null, config -> config.floatToStringSupport),
    FLOAT_TO_DECIMAL_FIXED(Schema.Type.FLOAT, null, Schema.Type.FIXED, LogicalTypes.decimal(4, 3), config -> config.floatToDecimalFixedSupport),
    FLOAT_TO_DECIMAL_BYTES(Schema.Type.FLOAT, null, Schema.Type.BYTES, LogicalTypes.decimal(4, 3), config -> config.floatToDecimalBytesSupport),

    DOUBLE_TO_DOUBLE(Schema.Type.DOUBLE, null, Schema.Type.DOUBLE, null, config -> true),
    DOUBLE_TO_STRING(Schema.Type.DOUBLE, null, Schema.Type.STRING, null, config -> config.doubleToStringSupport),
    DOUBLE_TO_DECIMAL_FIXED(Schema.Type.DOUBLE, null, Schema.Type.FIXED, LogicalTypes.decimal(4, 3), config -> config.doubleToDecimalFixedSupport),
    DOUBLE_TO_DECIMAL_BYTES(Schema.Type.DOUBLE, null, Schema.Type.BYTES, LogicalTypes.decimal(4, 3), config -> config.doubleToDecimalBytesSupport),

    STRING_TO_STRING(Schema.Type.STRING, null, Schema.Type.STRING, null, config -> true),
    STRING_TO_DECIMAL_FIXED(Schema.Type.STRING, null, Schema.Type.FIXED, LogicalTypes.decimal(4, 3), config -> config.stringToDecimalFixedSupport),
    STRING_TO_DECIMAL_BYTES(Schema.Type.STRING, null, Schema.Type.BYTES, LogicalTypes.decimal(4, 3), config -> config.stringToDecimalBytesSupport),
    STRING_TO_DATE(Schema.Type.STRING, null, Schema.Type.INT, LogicalTypes.date(), config -> config.stringToDateSupport),

    DECIMAL_FIXED_TO_STRING(Schema.Type.FIXED, LogicalTypes.decimal(4, 3), Schema.Type.STRING, null, config -> config.decimalFixedToStringSupport),
    DECIMAL_BYTES_TO_STRING(Schema.Type.BYTES, LogicalTypes.decimal(4, 3), Schema.Type.STRING, null, config -> config.decimalBytesToStringSupport),
    DATE_TO_STRING(Schema.Type.INT, LogicalTypes.date(), Schema.Type.STRING, null, config -> config.dateToStringSupport);

    public final Schema.Type before;
    public final Schema.Type after;
    public final LogicalType logicalTypeBefore;
    public final LogicalType logicalTypeAfter;
    public final Predicate<SchemaOnReadConfigs> isEnabled;

    SchemaOnReadTypePromotionCase(Schema.Type before, LogicalType logicalTypeBefore,
                                  Schema.Type after, LogicalType logicalTypeAfter,
                                  Predicate<SchemaOnReadConfigs> isEnabled) {
      this.before = before;
      this.logicalTypeBefore = logicalTypeBefore;
      this.after = after;
      this.logicalTypeAfter = logicalTypeAfter;
      this.isEnabled = isEnabled;
    }
  }

  private enum SchemaOnReadStructuralCase {

    REORDER_COLUMNS(
        c -> c.reorderColumnSupport,
        SchemaOnReadEvolutionTestUtils::addInitialReorderFields,
        SchemaOnReadEvolutionTestUtils::applyReorderModifications
    ),

    ADD_FIELD_NOT_AT_END(
        c -> c.addNewFieldNotAtEndSupport,
        SchemaOnReadEvolutionTestUtils::addInitialNonEndFields,
        SchemaOnReadEvolutionTestUtils::applyNonEndFieldModifications
    ),

    RENAME_COLUMN(
        c -> c.renameColumnSupport,
        SchemaOnReadEvolutionTestUtils::addInitialRenameFields,
        SchemaOnReadEvolutionTestUtils::applyRenameModifications
    ),

    REMOVE_COLUMN(
        c -> c.removeColumnSupport,
        SchemaOnReadEvolutionTestUtils::addInitialRemoveFields,
        SchemaOnReadEvolutionTestUtils::applyRemoveModifications
    ),

    RENAME_AS_REMOVED(
        c -> c.renameColumnAsPreviouslyRemovedSupport,
        SchemaOnReadEvolutionTestUtils::addInitialRenameAsRemovedFields,
        SchemaOnReadEvolutionTestUtils::applyRenameAsRemovedModifications
    ),

    ADD_FIELD_AT_END(
        c -> c.addNewFieldSupport,
        SchemaOnReadEvolutionTestUtils::addInitialEndFields,
        SchemaOnReadEvolutionTestUtils::applyEndFieldModifications
    );

    private final Function<SchemaOnReadConfigs, Boolean> isEnabled;
    private final TriFunction<InternalSchema, SchemaOnReadConfigs, SchemaModifier, InternalSchema> initializationFunction;
    private final QuadFunction<InternalSchema, SchemaOnReadConfigs, SchemaModifier, Integer, InternalSchema> updateFunction;

    SchemaOnReadStructuralCase(
        Function<SchemaOnReadConfigs, Boolean> isEnabled,
        TriFunction<InternalSchema, SchemaOnReadConfigs, SchemaModifier, InternalSchema> initializationFunction,
        QuadFunction<InternalSchema, SchemaOnReadConfigs, SchemaModifier, Integer, InternalSchema> updateFunction) {
      this.isEnabled = isEnabled;
      this.initializationFunction = initializationFunction;
      this.updateFunction = updateFunction;
    }

    public boolean enabled(SchemaOnReadConfigs configs) {
      return Boolean.TRUE.equals(isEnabled.apply(configs)); // Avoid NPE
    }

    public InternalSchema applyInitialization(InternalSchema schema, SchemaOnReadConfigs configs, SchemaModifier modifier) {
      return initializationFunction.apply(schema, configs, modifier);
    }

    public InternalSchema applyUpdate(InternalSchema schema, SchemaOnReadConfigs configs, SchemaModifier modifier, int iteration) {
      return updateFunction.apply(schema, configs, modifier, iteration);
    }


    @FunctionalInterface
    public interface QuadFunction<A, B, C, D, R> {
      R apply(A a, B b, C c, D d);
    }

    @FunctionalInterface
    public interface TriFunction<A, B, C, R> {
      R apply(A a, B b, C c);
    }
  }

  private static class FieldNameHolder {
    public final String baseFieldName;
    public final String nestedFieldName;
    public final String arrayFieldName;
    public final String mapFieldName;

    public FieldNameHolder(String baseFieldName, String nestedFieldName, String arrayFieldName, String mapFieldName) {
      this.baseFieldName = baseFieldName;
      this.nestedFieldName = nestedFieldName;
      this.arrayFieldName = arrayFieldName;
      this.mapFieldName = mapFieldName;
    }
  }

  // Field name generators for better maintainability
  private static class FieldNames {

    // Helper method to create FieldNameHolder with consistent naming pattern
    private static FieldNameHolder createFieldNameHolder(String operation, String suffix) {
      return new FieldNameHolder(
          "customField" + operation + suffix,
          "customFare" + operation + suffix,
          "customFieldArray" + operation + suffix,
          "customFieldMap" + operation + suffix
      );
    }

    static FieldNameHolder reorderField(int i) {
      return createFieldNameHolder("Reorder", String.valueOf(i));
    }

    static FieldNameHolder addField(int i) {
      return createFieldNameHolder("Add", String.valueOf(i));
    }

    static FieldNameHolder addEndField(int i) {
      return createFieldNameHolder("AddEnd", String.valueOf(i));
    }

    static FieldNameHolder renameField(int i, int revision) {
      return createFieldNameHolder("Rename", i + "r" + revision);
    }

    static FieldNameHolder removeField(int i) {
      return createFieldNameHolder("Remove", String.valueOf(i));
    }

    static FieldNameHolder removedForRenameField(int i) {
      return createFieldNameHolder("RemovedForRename", String.valueOf(i));
    }

    static FieldNameHolder renameToRemovedField(int i) {
      return createFieldNameHolder("RenameToRemoved", String.valueOf(i));
    }
  }

  // Helper class to reduce repetitive code
  private static class SchemaModifier {
    private final SchemaOnReadConfigs configs;

    // Structure definitions as enum
    private enum StructureType {
      BASE("", null, holder -> holder.baseFieldName),
      NESTED("fare.", "fare", holder -> holder.nestedFieldName),
      ARRAY("customFieldArray.element.", "customFieldArray.element", holder -> holder.arrayFieldName),
      MAP("customFieldMap.value.", "customFieldMap.value", holder -> holder.mapFieldName);

      final String prefix;
      final String parent; // for add operations
      final Function<FieldNameHolder, String> fieldExtractor;

      StructureType(String prefix, String parent,
                    Function<FieldNameHolder, String> fieldExtractor) {
        this.prefix = prefix;
        this.parent = parent;
        this.fieldExtractor = fieldExtractor;
      }

      boolean isEnabled(SchemaOnReadConfigs configs) {
        switch (this) {
          case BASE: return true;
          case NESTED: return configs.nestedSupport;
          case ARRAY: return configs.arraySupport;
          case MAP: return configs.mapSupport;
          default: return false;
        }
      }
    }

    private SchemaModifier(SchemaOnReadConfigs configs) {
      this.configs = configs;
    }

    private InternalSchema addFields(InternalSchema schema, List<FieldNameHolder> fieldNamesList) {
      TableChanges.ColumnAddChange columnAddChange = TableChanges.ColumnAddChange.get(schema);

      for (FieldNameHolder fieldNames : fieldNamesList) {
        for (StructureType structure : StructureType.values()) {
          if (structure.isEnabled(configs)) {
            String fieldName = structure.fieldExtractor.apply(fieldNames);
            if (structure.parent == null) {
              columnAddChange.addColumns(fieldName, Types.StringType.get(), "");
            } else {
              columnAddChange.addColumns(structure.parent, fieldName, Types.StringType.get(), "");
            }
          }
        }
      }

      return SchemaChangeUtils.applyTableChanges2Schema(schema, columnAddChange);
    }

    private InternalSchema deleteFields(InternalSchema schema, FieldNameHolder fieldNames) {
      TableChanges.ColumnDeleteChange columnDeleteChange = TableChanges.ColumnDeleteChange.get(schema);
      for (StructureType structure : StructureType.values()) {
        if (structure.isEnabled(configs)) {
          String fieldName = structure.fieldExtractor.apply(fieldNames);
          columnDeleteChange.deleteColumn(structure.prefix + fieldName);
        }
      }
      return SchemaChangeUtils.applyTableChanges2Schema(schema, columnDeleteChange);
    }

    private InternalSchema renameFields(InternalSchema schema,
                                       FieldNameHolder oldFieldNames,
                                       FieldNameHolder newFieldNames) {
      TableChanges.ColumnUpdateChange columnUpdateChange = TableChanges.ColumnUpdateChange.get(schema);
      for (StructureType structure : StructureType.values()) {
        if (structure.isEnabled(configs)) {
          String oldFieldName = structure.fieldExtractor.apply(oldFieldNames);
          String newFieldName = structure.fieldExtractor.apply(newFieldNames);
          columnUpdateChange.renameColumn(structure.prefix + oldFieldName, newFieldName);
        }
      }

      return SchemaChangeUtils.applyTableChanges2Schema(schema, columnUpdateChange);
    }

    private InternalSchema repositionFields(InternalSchema schema,
                                           FieldNameHolder fieldNames,
                                           FieldNameHolder refFieldNames,
                                           TableChange.ColumnPositionChange.ColumnPositionType positionType) {
      TableChanges.ColumnUpdateChange columnUpdateChange = TableChanges.ColumnUpdateChange.get(schema);
      for (StructureType structure : StructureType.values()) {
        if (structure.isEnabled(configs)) {
          String fieldName = structure.fieldExtractor.apply(fieldNames);
          String refFieldName = structure.fieldExtractor.apply(refFieldNames);
          columnUpdateChange.addPositionChange(
              structure.prefix + fieldName,
              structure.prefix + refFieldName,
              positionType);
        }
      }
      return SchemaChangeUtils.applyTableChanges2Schema(schema, columnUpdateChange);
    }
  }



  private static void validateConfigs(SchemaOnReadConfigs configs) {
    if (configs.renameColumnAsPreviouslyRemovedSupport) {
      if (!configs.renameColumnSupport && !configs.removeColumnSupport) {
        throw new IllegalArgumentException(
            "renameColumnAsPreviouslyRemovedSupport requires renameColumnSupport or removeColumnSupport to be enabled");
      }
    }
  }

  private static List<Pair<Schema.Type, LogicalType>> buildBaseFieldsList(SchemaOnReadConfigs configs, int iteration) {
    List<Pair<Schema.Type, LogicalType>> baseFields = new ArrayList<>();
    for (int i = 0; i < configs.numRoundsSupported; i++) {
      for (SchemaOnReadTypePromotionCase evolution : SchemaOnReadTypePromotionCase.values()) {
        if (evolution.isEnabled.test(configs)) {
          if (i >= iteration) {
            baseFields.add(Pair.of(evolution.before, evolution.logicalTypeBefore));
          } else {
            baseFields.add(Pair.of(evolution.after, evolution.logicalTypeAfter));
          }
        }
      }
    }
    return baseFields;
  }

  private static InternalSchema addInitialFields(InternalSchema schema, SchemaOnReadConfigs configs, SchemaModifier modifier) {
    InternalSchema result = schema;

    for (SchemaOnReadStructuralCase strategy : SchemaOnReadStructuralCase.values()) {
      if (strategy.enabled(configs)) {
        result = strategy.applyInitialization(result, configs, modifier);
      }
    }

    return result;
  }

  private static InternalSchema addInitialReorderFields(InternalSchema schema, SchemaOnReadConfigs configs, SchemaModifier modifier) {
    // Collects all FieldNameHolders into a list
    List<FieldNameHolder> fieldNamesList = new ArrayList<>();
    for (int i = 0; i < configs.numRoundsSupported; i++) {
      fieldNamesList.add(FieldNames.reorderField(i));
    }
    // Calls new modifier method that takes schema and List<FieldNameHolder>
    return modifier.addFields(schema, fieldNamesList);
  }

  private static InternalSchema addInitialNonEndFields(InternalSchema schema, SchemaOnReadConfigs configs, SchemaModifier modifier) {
    return modifier.addFields(schema, Collections.singletonList(FieldNames.addField(0)));
  }

  private static InternalSchema addInitialRenameFields(InternalSchema schema, SchemaOnReadConfigs configs, SchemaModifier modifier) {
    List<FieldNameHolder> renameFields = new ArrayList<>(configs.numRoundsSupported );
    for (int i = 0; i < configs.numRoundsSupported; i++) {
      renameFields.add(FieldNames.renameField(i, 0));
    }

    return modifier.addFields(schema, renameFields);
  }

  private static InternalSchema addInitialRemoveFields(InternalSchema schema, SchemaOnReadConfigs configs, SchemaModifier modifier) {
    List<FieldNameHolder> removeFields = new ArrayList<>(configs.numRoundsSupported - 1);
    for (int i = 0; i < configs.numRoundsSupported - 1; i++) {
      removeFields.add(FieldNames.removeField(i));
    }
    return modifier.addFields(schema, removeFields);
  }

  private static InternalSchema addInitialRenameAsRemovedFields(InternalSchema schema, SchemaOnReadConfigs configs, SchemaModifier modifier) {
    List<FieldNameHolder> renameAsRemovedFields = new ArrayList<>(configs.numRoundsSupported - 1);
    for (int i = 0; i < configs.numRoundsSupported - 1; i++) {
      renameAsRemovedFields.add(FieldNames.removedForRenameField(i));
      renameAsRemovedFields.add(FieldNames.renameToRemovedField(i));
    }
    return modifier.addFields(schema, renameAsRemovedFields);
  }

  private static InternalSchema addInitialEndFields(InternalSchema schema, SchemaOnReadConfigs configs, SchemaModifier modifier) {
    return modifier.addFields(schema, Collections.singletonList(FieldNames.addEndField(0)));
  }

  private static InternalSchema applyIterativeModifications(InternalSchema schema, SchemaOnReadConfigs configs,
                                                     SchemaModifier modifier, int iteration) {
    InternalSchema result = schema;
    for (int i = 1; i <= iteration; i++) {
      result = applyIterationModifications(result, configs, modifier, i);
    }
    return result;
  }



  private static InternalSchema applyIterationModifications(InternalSchema schema, SchemaOnReadConfigs configs,
                                                     SchemaModifier modifier, int iteration) {
    InternalSchema result = schema;

    for (SchemaOnReadStructuralCase strategy : SchemaOnReadStructuralCase.values()) {
      if (strategy.enabled(configs)) {
        result = strategy.applyUpdate(result, configs, modifier, iteration);
      }
    }

    return result;
  }

  private static InternalSchema applyReorderModifications(InternalSchema schema, SchemaOnReadConfigs configs,
                                                          SchemaModifier modifier, int iteration) {
    int sourceIndex = configs.numRoundsSupported - iteration;
    int targetIndex = (configs.numRoundsSupported + 1 - iteration) % configs.numRoundsSupported;

    FieldNameHolder sourceFields = FieldNames.reorderField(sourceIndex);
    FieldNameHolder targetFields = FieldNames.reorderField(targetIndex);

    return modifier.repositionFields(schema, sourceFields, targetFields,
        TableChange.ColumnPositionChange.ColumnPositionType.BEFORE);
  }

  private static InternalSchema applyNonEndFieldModifications(InternalSchema schema, SchemaOnReadConfigs configs,
                                                              SchemaModifier modifier, int iteration) {
    // Add new fields
    InternalSchema tempSchema = modifier.addFields(schema, Collections.singletonList(FieldNames.addField(iteration)));

    // Position the new fields
    FieldNameHolder currentFields = FieldNames.addField(iteration);
    FieldNameHolder previousFields = FieldNames.addField(iteration - 1);

   return modifier.repositionFields(tempSchema,
        currentFields, previousFields,
        TableChange.ColumnPositionChange.ColumnPositionType.AFTER);
  }

  private static InternalSchema applyRenameModifications(InternalSchema schema, SchemaOnReadConfigs configs,
                                                         SchemaModifier modifier, int iteration) {
    InternalSchema result = schema;

    for (int j = 0; j < iteration; j++) {
      int oldRevision = iteration - 1 - j;
      int newRevision = iteration - j;
      FieldNameHolder oldFields = FieldNames.renameField(j, oldRevision);
      FieldNameHolder newFields = FieldNames.renameField(j, newRevision);
      result = modifier.renameFields(result, oldFields, newFields);
    }

    return result;
  }

  private static InternalSchema applyRemoveModifications(InternalSchema schema, SchemaOnReadConfigs configs,
                                                         SchemaModifier modifier, int iteration) {
    int indexToRemove = iteration - 1;
    FieldNameHolder fieldsToRemove = FieldNames.removeField(indexToRemove);
    return modifier.deleteFields(schema, fieldsToRemove);
  }

  private static InternalSchema applyRenameAsRemovedModifications(InternalSchema schema, SchemaOnReadConfigs configs,
                                                                  SchemaModifier modifier, int iteration) {
    int index = iteration - 1;

    // First delete the fields
    FieldNameHolder fieldsToDelete = FieldNames.removedForRenameField(index);
    InternalSchema tempSchema = modifier.deleteFields(schema, fieldsToDelete);

    // Then rename the other fields
    FieldNameHolder oldFields = FieldNames.renameToRemovedField(index);
    FieldNameHolder newFields = FieldNames.removedForRenameField(index);

    return modifier.renameFields(tempSchema, oldFields, newFields);
  }

  private static InternalSchema applyEndFieldModifications(InternalSchema schema, SchemaOnReadConfigs configs,
                                                           SchemaModifier modifier, int iteration) {
    FieldNameHolder endFields = FieldNames.addEndField(iteration);
    return modifier.addFields(schema, Collections.singletonList(endFields));
  }
}
