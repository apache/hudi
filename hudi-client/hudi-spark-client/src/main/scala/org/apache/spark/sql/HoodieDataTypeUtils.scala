package org.apache.spark.sql

import org.apache.spark.sql.types._

object HoodieDataTypeUtils {

  /**
   * Parses provided [[jsonSchema]] into [[StructType]].
   *
   * Throws [[RuntimeException]] in case it's unable to parse it as such.
   */
  def parseStructTypeFromJson(jsonSchema: String): StructType =
    StructType.fromString(jsonSchema)

  /**
   * Checks whether provided {@link DataType} contains {@link DecimalType} whose scale is less than
   * {@link Decimal# MAX_LONG_DIGITS ( )}
   */
  def hasSmallPrecisionDecimalType(sparkType: DataType): Boolean = {
    sparkType match {
      case st: StructType =>
        st.exists(f => hasSmallPrecisionDecimalType(f.dataType))

      case map: MapType =>
        hasSmallPrecisionDecimalType(map.keyType) ||
          hasSmallPrecisionDecimalType(map.valueType)

      case at: ArrayType =>
        hasSmallPrecisionDecimalType(at.elementType)

      case dt: DecimalType =>
        dt.precision < Decimal.MAX_LONG_DIGITS

      case _ => false
    }
  }
}
