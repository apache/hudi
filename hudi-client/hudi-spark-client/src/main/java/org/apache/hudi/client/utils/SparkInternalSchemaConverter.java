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

package org.apache.hudi.client.utils;

import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.internal.schema.action.InternalSchemaMerger;
import org.apache.hudi.internal.schema.utils.InternalSchemaUtils;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.ArrayType$;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BinaryType$;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.BooleanType$;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.CharType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DateType$;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DecimalType$;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.DoubleType$;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.FloatType$;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.MapType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructType$;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.types.TimestampType$;
import org.apache.spark.sql.types.UserDefinedType;
import org.apache.spark.sql.types.VarcharType;

import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class SparkInternalSchemaConverter {
  private SparkInternalSchemaConverter() {

  }

  public static final String HOODIE_QUERY_SCHEMA = "hoodie.schema.internal.querySchema";
  public static final String HOODIE_TABLE_PATH = "hoodie.tablePath";
  public static final String HOODIE_VALID_COMMITS_LIST = "hoodie.valid.commits.list";

  /**
   * Converts a spark schema to an hudi internal schema. Fields without IDs are kept and assigned fallback IDs.
   *
   * @param sparkSchema a spark schema
   * @return a matching internal schema for the provided spark schema
   */
  public static InternalSchema convertStructTypeToInternalSchema(StructType sparkSchema) {
    Type newType = buildTypeFromStructType(sparkSchema, true, new AtomicInteger(0));
    return new InternalSchema(((Types.RecordType)newType).fields());
  }

  public static Type buildTypeFromStructType(DataType sparkType, Boolean firstVisitRoot, AtomicInteger nextId) {
    if (sparkType instanceof StructType) {
      StructField[] fields = ((StructType) sparkType).fields();
      int nextAssignId = firstVisitRoot ? 0 : nextId.get();
      nextId.set(nextAssignId + fields.length);
      List<Type> newTypes = new ArrayList<>();
      for (StructField f : fields) {
        newTypes.add(buildTypeFromStructType(f.dataType(), false, nextId));
      }
      List<Types.Field> newFields = new ArrayList<>();
      for (int i = 0; i < newTypes.size(); i++) {
        StructField f = fields[i];
        newFields.add(Types.Field.get(nextAssignId + i, f.nullable(), f.name(), newTypes.get(i),
            f.getComment().isDefined() ? f.getComment().get() : null));
      }
      return Types.RecordType.get(newFields);
    } else if (sparkType instanceof MapType) {
      MapType map = (MapType) sparkType;
      DataType keyType = map.keyType();
      DataType valueType = map.valueType();
      int keyId = nextId.get();
      int valueId = keyId + 1;
      nextId.set(valueId + 1);
      return Types.MapType.get(keyId, valueId, buildTypeFromStructType(keyType, false, nextId),
          buildTypeFromStructType(valueType, false, nextId), map.valueContainsNull());
    } else if (sparkType instanceof ArrayType) {
      ArrayType array = (ArrayType) sparkType;
      DataType et = array.elementType();
      int elementId = nextId.get();
      nextId.set(elementId + 1);
      return Types.ArrayType.get(elementId, array.containsNull(), buildTypeFromStructType(et, false, nextId));
    } else if (sparkType instanceof UserDefinedType) {
      throw new UnsupportedOperationException("User-defined types are not supported");
    } else if (sparkType instanceof BooleanType) {
      return Types.BooleanType.get();
    } else if (sparkType instanceof IntegerType
        || sparkType instanceof ShortType
        || sparkType instanceof ByteType) {
      return Types.IntType.get();
    } else if (sparkType instanceof LongType) {
      return Types.LongType.get();
    } else if (sparkType instanceof FloatType) {
      return Types.FloatType.get();
    } else if (sparkType instanceof DoubleType) {
      return Types.DoubleType.get();
    } else if (sparkType instanceof StringType
        || sparkType instanceof CharType
        || sparkType instanceof VarcharType) {
      return Types.StringType.get();
    } else if (sparkType instanceof DateType) {
      return Types.DateType.get();
      // spark 3.3.0 support TimeStampNTZ, to do support spark3.3.0
    } else if (sparkType instanceof TimestampType) {
      return Types.TimestampType.get();
    } else if (sparkType instanceof DecimalType) {
      return Types.DecimalType.get(
          ((DecimalType) sparkType).precision(),
          ((DecimalType) sparkType).scale());
    } else if (sparkType instanceof BinaryType) {
      return Types.BinaryType.get();
    } else {
      throw new UnsupportedOperationException(String.format("Not a supported type: %s",  sparkType.catalogString()));
    }
  }

  /**
   * Converts Spark schema to Hudi internal schema, and prune fields.
   * Fields without IDs are kept and assigned fallback IDs.
   *
   * @param sparkSchema a pruned spark schema
   * @param originSchema a internal schema for hoodie table
   * @return a pruned internal schema for the provided spark schema
   */
  public static InternalSchema convertAndPruneStructTypeToInternalSchema(StructType sparkSchema, InternalSchema originSchema) {
    List<String> pruneNames = collectColNamesFromSparkStruct(sparkSchema);
    return InternalSchemaUtils.pruneInternalSchema(originSchema, pruneNames);
  }

  /**
   * Collect all the leaf nodes names.
   *
   * @param sparkSchema a spark schema
   * @return leaf nodes full names.
   */
  public static List<String> collectColNamesFromSparkStruct(StructType sparkSchema) {
    List<String> result =  new ArrayList<>();
    collectColNamesFromStructType(sparkSchema, new LinkedList<>(), result);
    return result;
  }

  private static void collectColNamesFromStructType(DataType sparkType, Deque<String> fieldNames, List<String> resultSet) {
    if (sparkType instanceof StructType) {
      StructField[] fields = ((StructType) sparkType).fields();
      for (StructField f : fields) {
        fieldNames.push(f.name());
        collectColNamesFromStructType(f.dataType(), fieldNames, resultSet);
        fieldNames.pop();
        addFullName(f.dataType(), f.name(), fieldNames, resultSet);
      }
    } else if (sparkType instanceof MapType) {
      MapType map = (MapType) sparkType;
      DataType keyType = map.keyType();
      DataType valueType = map.valueType();
      // key
      fieldNames.push("key");
      collectColNamesFromStructType(keyType, fieldNames, resultSet);
      fieldNames.pop();
      addFullName(keyType,"key", fieldNames, resultSet);
      // value
      fieldNames.push("value");
      collectColNamesFromStructType(valueType, fieldNames, resultSet);
      fieldNames.poll();
      addFullName(valueType,"value", fieldNames, resultSet);
    } else if (sparkType instanceof ArrayType) {
      ArrayType array = (ArrayType) sparkType;
      DataType et = array.elementType();
      fieldNames.push("element");
      collectColNamesFromStructType(et, fieldNames, resultSet);
      fieldNames.pop();
      addFullName(et, "element", fieldNames, resultSet);
    } else if (sparkType instanceof UserDefinedType) {
      throw new UnsupportedOperationException("User-defined types are not supported");
    } else {
      // do nothings
    }
  }

  private static void addFullName(DataType sparkType, String name, Deque<String> fieldNames, List<String> resultSet) {
    if (!(sparkType instanceof StructType) && !(sparkType instanceof ArrayType) && !(sparkType instanceof MapType)) {
      resultSet.add(InternalSchemaUtils.createFullName(name, fieldNames));
    }
  }

  public static StructType mergeSchema(InternalSchema fileSchema, InternalSchema querySchema) {
    InternalSchema schema = new InternalSchemaMerger(fileSchema, querySchema, true, true).mergeSchema();
    return constructSparkSchemaFromInternalSchema(schema);
  }

  public static Map<Integer, Pair<DataType, DataType>> collectTypeChangedCols(InternalSchema schema, InternalSchema other) {
    return InternalSchemaUtils
        .collectTypeChangedCols(schema, other)
        .entrySet()
        .stream()
        .collect(Collectors.toMap(e -> e.getKey(), e -> Pair.of(constructSparkSchemaFromType(e.getValue().getLeft()), constructSparkSchemaFromType(e.getValue().getRight()))));
  }

  public static StructType constructSparkSchemaFromInternalSchema(InternalSchema schema) {
    return (StructType) constructSparkSchemaFromType(schema.getRecord());
  }

  private static DataType constructSparkSchemaFromType(Type type) {
    switch (type.typeId()) {
      case RECORD:
        Types.RecordType record = (Types.RecordType) type;
        List<Types.Field> fields = record.fields();
        List<StructField> structFields = new ArrayList<>();
        for (Types.Field f : fields) {
          DataType dataType = constructSparkSchemaFromType(f.type());
          StructField structField = StructField.apply(f.name(), dataType, f.isOptional(), Metadata.empty());
          structField = f.doc() == null ? structField : structField.withComment(f.doc());
          structFields.add(structField);
        }
        return StructType$.MODULE$.apply(structFields);
      case ARRAY:
        Types.ArrayType array = (Types.ArrayType) type;
        DataType elementType = constructSparkSchemaFromType(array.elementType());
        return ArrayType$.MODULE$.apply(elementType, array.isElementOptional());
      case MAP:
        Types.MapType map = (Types.MapType) type;
        DataType keyDataType = constructSparkSchemaFromType(map.keyType());
        DataType valueDataType = constructSparkSchemaFromType(map.valueType());
        return MapType$.MODULE$.apply(keyDataType, valueDataType, map.isValueOptional());
      case BOOLEAN:
        return BooleanType$.MODULE$;
      case INT:
        return IntegerType$.MODULE$;
      case LONG:
        return LongType$.MODULE$;
      case FLOAT:
        return FloatType$.MODULE$;
      case DOUBLE:
        return DoubleType$.MODULE$;
      case DATE:
        return DateType$.MODULE$;
      case TIME:
        throw new UnsupportedOperationException(String.format("cannot convert %s type to Spark", type));
      case TIMESTAMP:
        // todo support TimeStampNTZ
        return TimestampType$.MODULE$;
      case STRING:
        return StringType$.MODULE$;
      case UUID:
        return StringType$.MODULE$;
      case FIXED:
        return BinaryType$.MODULE$;
      case BINARY:
        return BinaryType$.MODULE$;
      case DECIMAL:
        Types.DecimalType decimal = (Types.DecimalType) type;
        return DecimalType$.MODULE$.apply(decimal.precision(), decimal.scale());
      default:
        throw new UnsupportedOperationException(String.format("cannot convert unknown type: %s to Spark", type));
    }
  }

  /**
   * Convert Int/long type to other Type.
   * Now only support int/long -> long/float/double/string
   * TODO: support more types
   */
  private static boolean convertIntLongType(WritableColumnVector oldV, WritableColumnVector newV, DataType newType, int len) {
    boolean isInt = oldV.dataType() instanceof IntegerType;
    if (newType instanceof LongType || newType instanceof FloatType
        || newType instanceof DoubleType || newType instanceof StringType || newType instanceof DecimalType) {
      for (int i = 0; i < len; i++) {
        if (oldV.isNullAt(i)) {
          newV.putNull(i);
          continue;
        }
        // int/long -> long/float/double/string/decimal
        if (newType instanceof LongType) {
          newV.putLong(i, isInt ? oldV.getInt(i) : oldV.getLong(i));
        } else if (newType instanceof FloatType) {
          newV.putFloat(i, isInt ? oldV.getInt(i) : oldV.getLong(i));
        } else if (newType instanceof DoubleType) {
          newV.putDouble(i, isInt ? oldV.getInt(i) : oldV.getLong(i));
        } else if (newType instanceof StringType) {
          newV.putByteArray(i, ((isInt ? oldV.getInt(i) : oldV.getLong(i)) + "").getBytes(StandardCharsets.UTF_8));
        } else if (newType instanceof DecimalType) {
          Decimal oldDecimal = Decimal.apply(isInt ? oldV.getInt(i) : oldV.getLong(i));
          oldDecimal.changePrecision(((DecimalType) newType).precision(), ((DecimalType) newType).scale());
          newV.putDecimal(i, oldDecimal, ((DecimalType) newType).precision());
        }
      }
      return true;
    }
    return false;
  }

  /**
   * Convert float type to other Type.
   * Now only support float -> double/String
   * TODO: support more types
   */
  private static boolean convertFloatType(WritableColumnVector oldV, WritableColumnVector newV, DataType newType, int len) {
    if (newType instanceof DoubleType || newType instanceof StringType || newType instanceof DecimalType) {
      for (int i = 0; i < len; i++) {
        if (oldV.isNullAt(i)) {
          newV.putNull(i);
          continue;
        }
        // float -> double/string/decimal
        if (newType instanceof DoubleType) {
          newV.putDouble(i, Double.valueOf(oldV.getFloat(i) + ""));
        } else if (newType instanceof StringType) {
          newV.putByteArray(i, (oldV.getFloat(i) + "").getBytes(StandardCharsets.UTF_8));
        } else if (newType instanceof DecimalType) {
          Decimal oldDecimal = Decimal.apply(oldV.getFloat(i));
          oldDecimal.changePrecision(((DecimalType) newType).precision(), ((DecimalType) newType).scale());
          newV.putDecimal(i, oldDecimal, ((DecimalType) newType).precision());
        }
      }
      return true;
    }
    return false;
  }

  /**
   * Convert double type to other Type.
   * Now only support Double -> Decimal/String
   * TODO: support more types
   */
  private static boolean convertDoubleType(WritableColumnVector oldV, WritableColumnVector newV, DataType newType, int len) {
    if (newType instanceof DecimalType || newType instanceof StringType) {
      for (int i = 0; i < len; i++) {
        if (oldV.isNullAt(i)) {
          newV.putNull(i);
          continue;
        }
        // double -> decimal/string
        if (newType instanceof DecimalType) {
          Decimal oldDecimal = Decimal.apply(oldV.getDouble(i));
          oldDecimal.changePrecision(((DecimalType) newType).precision(), ((DecimalType) newType).scale());
          newV.putDecimal(i, oldDecimal, ((DecimalType) newType).precision());
        } else if (newType instanceof StringType) {
          newV.putByteArray(i, (oldV.getDouble(i) + "").getBytes(StandardCharsets.UTF_8));
        }
      }
      return true;
    }
    return false;
  }

  /**
   * Convert decimal type to other Type.
   * Now only support Decimal -> Decimal/String
   * TODO: support more types
   */
  private static boolean convertDecimalType(WritableColumnVector oldV, WritableColumnVector newV, DataType newType, int len) {
    DataType oldType = oldV.dataType();
    if (newType instanceof DecimalType || newType instanceof StringType) {
      for (int i = 0; i < len; i++) {
        if (oldV.isNullAt(i)) {
          newV.putNull(i);
          continue;
        }
        Decimal oldDecimal = oldV.getDecimal(i, ((DecimalType) oldType).precision(), ((DecimalType) oldType).scale());
        if (newType instanceof DecimalType) {
          oldDecimal.changePrecision(((DecimalType) newType).precision(), ((DecimalType) newType).scale());
          newV.putDecimal(i, oldDecimal, ((DecimalType) newType).precision());
        } else if (newType instanceof StringType) {
          newV.putByteArray(i, oldDecimal.toString().getBytes(StandardCharsets.UTF_8));
        }
      }
      return true;
    }
    return false;
  }

  /**
   * Convert date type to other Type.
   * Now only support Date -> String
   * TODO: support more types
   */
  private static boolean convertDateType(WritableColumnVector oldV, WritableColumnVector newV, DataType newType, int len) {
    if (newType instanceof StringType) {
      for (int i = 0; i < len; i++) {
        if (oldV.isNullAt(i)) {
          newV.putNull(i);
          continue;
        }
        // to do support rebaseDate
        String res = org.apache.spark.sql.catalyst.util.DateTimeUtils.toJavaDate(oldV.getInt(i)).toString();
        newV.putByteArray(i, res.getBytes(StandardCharsets.UTF_8));
      }
      return true;
    }
    return false;
  }

  /**
   * Convert String type to other Type.
   * Now only support String -> Decimal/Date.
   * Notice: This convert maybe failed!!!
   * TODO: support more types
   */
  private static boolean convertStringType(WritableColumnVector oldV, WritableColumnVector newV, DataType newType, int len) {
    if (newType instanceof DateType || newType instanceof DecimalType) {
      for (int i = 0; i < len; i++) {
        if (oldV.isNullAt(i)) {
          newV.putNull(i);
          continue;
        }
        // to do support rebaseDate
        if (newType instanceof DateType) {
          int days = org.apache.spark.sql.catalyst.util.DateTimeUtils.fromJavaDate(Date.valueOf(oldV.getUTF8String(i).toString()));
          newV.putInt(i, days);
        } else if (newType instanceof DecimalType) {
          DecimalType decimalType = (DecimalType) newType;
          java.math.BigDecimal bigDecimal = new java.math.BigDecimal(oldV.getUTF8String(i).toString().trim());
          Decimal sparkDecimal = Decimal.apply(bigDecimal);
          sparkDecimal.changePrecision(decimalType.precision(), decimalType.scale());
          newV.putDecimal(i, sparkDecimal, decimalType.precision());
        }
      }
      return true;
    }
    return false;
  }

  public static boolean convertColumnVectorType(WritableColumnVector oldV, WritableColumnVector newV, int len) {
    if (len == 0 || oldV == null || newV == null) {
      return false;
    }
    DataType oldType = oldV.dataType();  // old colType eg: floatType
    DataType newType = newV.dataType();  // new colType eg: doubleType
    if (oldV != null && newType != null) {
      if (oldType instanceof BooleanType) {
        return false;
      } else if (oldType instanceof ByteType) {
        return false;
      } else if (oldType instanceof ShortType) {
        return false;
      } else if (oldType instanceof IntegerType) {
        return convertIntLongType(oldV, newV, newType, len);
      } else if (oldType instanceof LongType) {
        return convertIntLongType(oldV, newV, newType, len);
      } else if (oldType instanceof FloatType) {
        return convertFloatType(oldV, newV, newType, len);
      } else if (oldType instanceof DoubleType) {
        return convertDoubleType(oldV, newV, newType, len);
      } else if (oldType instanceof StringType) {
        return convertStringType(oldV, newV, newType, len);
      } else if (oldType instanceof BinaryType) {
        return false;
      } else if (oldType instanceof DecimalType) {
        return convertDecimalType(oldV, newV, newType, len);
      } else if (oldType instanceof DateType) {
        return convertDateType(oldV, newV, newType, len);
      } else if (oldType instanceof TimestampType) {
        return false;
      } else {
        throw new UnsupportedOperationException("Datatype not supported " + oldV);
      }
    }
    return false;
  }
}

