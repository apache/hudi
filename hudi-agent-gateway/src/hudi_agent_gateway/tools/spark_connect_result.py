# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Encode Spark Connect's execution schema and Arrow batches for gateway tools."""

import base64
import math
from collections.abc import Callable
from datetime import date, datetime, timedelta
from typing import Any

import pyarrow as pa
from pyspark.sql import types as T
from pyspark.sql.conversion import ArrowTableToRowsConversion
from pyspark.sql.pandas.types import from_arrow_schema

from hudi_agent_gateway.tools.lakehouse_client import QueryResult


def _field_name(name: str) -> str:
    # Quoting every field works under both regular and ANSI keyword rules.
    return "`" + name.replace("`", "``") + "`"


def spark_sql_type(dtype: T.DataType, *, inferred: bool = False) -> str:
    """Format a result type, omitting TIME precision when only Arrow supplied the schema."""
    if isinstance(dtype, T.StructType):
        fields = (f"{_field_name(f.name)}:{spark_sql_type(f.dataType, inferred=inferred)}"
                  for f in dtype)
        return f"struct<{','.join(fields)}>"
    if isinstance(dtype, T.ArrayType):
        return f"array<{spark_sql_type(dtype.elementType, inferred=inferred)}>"
    if isinstance(dtype, T.MapType):
        key = spark_sql_type(dtype.keyType, inferred=inferred)
        value = spark_sql_type(dtype.valueType, inferred=inferred)
        return f"map<{key},{value}>"
    if inferred and isinstance(dtype, T.TimeType):
        return "time"
    return dtype.simpleString()


def _float_value(number: float) -> float | str:
    if math.isfinite(number):
        return number
    if math.isnan(number):
        return "NaN"
    return "Infinity" if number > 0 else "-Infinity"


def _iso_date(days: int) -> str:
    # Gregorian dates repeat every 400 years. Keep the date used by Python
    # within 1970..2369, including for negative epoch days and year zero.
    cycles, day = divmod(days, 146097)
    within_cycle = date(1970, 1, 1) + timedelta(days=day)
    year = within_cycle.year + 400 * cycles
    sign = "-" if year < 0 else "+" if year > 9999 else ""
    return f"{sign}{abs(year):04}-{within_cycle.month:02}-{within_cycle.day:02}"


def _iso_timestamp(micros: int) -> str:
    days, within_day = divmod(micros, 86_400_000_000)
    clock = (datetime.min + timedelta(microseconds=within_day)).time()
    return f"{_iso_date(days)}T{clock.isoformat()}"


def _trim_fraction(text: str) -> str:
    return text.rstrip("0").rstrip(".") if "." in text else text


def _encoder(dtype: T.DataType, *, map_key: bool = False) -> Callable[[pa.Scalar], Any]:
    encode = _nonnull_encoder(dtype, map_key=map_key)
    return lambda v: encode(v) if v.is_valid else None


def _nonnull_encoder(dtype: T.DataType, *, map_key: bool) -> Callable[[pa.Scalar], Any]:
    if isinstance(dtype, T.StructType):
        fields = [_encoder(f.dataType, map_key=map_key) for f in dtype]
        names = dtype.names
        # Bind fields by position: the wire schema can rename duplicate fields.
        if len(set(names)) != len(names):
            return lambda v: [encode(v[i]) for i, encode in enumerate(fields)]
        return lambda v: {name: fields[i](v[i]) for i, name in enumerate(names)}
    if isinstance(dtype, T.ArrayType):
        element = _encoder(dtype.elementType, map_key=map_key)
        return lambda v: [element(item) for item in v.values]
    if isinstance(dtype, T.MapType):
        item = _encoder(dtype.valueType)
        if isinstance(dtype.keyType, (T.StringType, T.IntegralType)):
            return lambda v: {str(entry[0].as_py()): item(entry[1]) for entry in v.values}
        key = _encoder(dtype.keyType, map_key=True)
        return lambda v: [[key(entry[0]), item(entry[1])] for entry in v.values]
    if map_key and isinstance(dtype, T.LongType):
        return lambda v: str(v.as_py())
    if isinstance(dtype, T.TimestampType):
        # Spark sends epoch microseconds. Arrow's timezone only controls display.
        return lambda v: _iso_timestamp(v.value) + "+00:00"
    if isinstance(dtype, T.TimestampNTZType):
        return lambda v: _iso_timestamp(v.value)
    if isinstance(dtype, T.DateType):
        return lambda v: _iso_date(v.value)
    if isinstance(dtype, T.TimeType):
        return lambda v: _trim_fraction(v.cast(pa.string()).as_py())
    if isinstance(dtype, (T.FloatType, T.DoubleType)):
        return lambda v: _float_value(v.as_py())
    if isinstance(dtype, T.DecimalType):
        return lambda v: format(v.as_py(), "f")
    if isinstance(dtype, T.BinaryType):
        return lambda v: base64.b64encode(v.as_py()).decode("ascii")
    # Preserve collect() conversion for remaining Spark types.
    convert = ArrowTableToRowsConversion._create_converter(dtype, none_on_identity=True)
    if convert is not None:
        return lambda v: convert(v.as_py())
    return lambda v: v.as_py()


def encode_result(table: pa.Table, schema: T.StructType | None) -> QueryResult:
    inferred = schema is None
    if schema is None:
        # Same fallback as PySpark collect(), using only this execution's Arrow schema.
        schema = from_arrow_schema(table.schema, prefer_timestamp_ntz=True)
    rows: list[list[Any]] = [[] for _ in range(table.num_rows)]
    for field, column in zip(schema, table.columns, strict=True):
        encode = _encoder(field.dataType)
        for row, value in zip(rows, column, strict=True):
            row.append(encode(value))
    return QueryResult(
        columns=schema.names,
        column_types=[spark_sql_type(f.dataType, inferred=inferred) for f in schema],
        rows=rows,
    )
