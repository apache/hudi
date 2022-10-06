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

package org.apache.spark.sql.hudi

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.twitter.chill.KSerializer
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import org.apache.commons.io.IOUtils
import org.apache.hudi.HoodieInternalRowUtils
import org.apache.hudi.commmon.model.HoodieSparkRecord
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils
import org.apache.spark.SparkEnv
import scala.collection.mutable

/**
 * Custom serializer used for generic spark records. If the user registers the schemas
 * ahead of time, then the schema's fingerprint will be sent with each message instead of the actual
 * schema, as to reduce network IO.
 * Actions like parsing or compressing schemas are computationally expensive so the serializer
 * caches all previously seen values as to reduce the amount of work needed to do.
 */
class HoodieSparkRecordSerializer() extends KSerializer[HoodieSparkRecord] {
  /** Used to reduce the amount of effort to compress the schema */
  private val compressCache = new mutable.HashMap[StructType, Array[Byte]]()
  private val decompressCache = new mutable.HashMap[ByteBuffer, StructType]()

  // GenericAvroSerializer can't take a SparkConf in the constructor b/c then it would become
  // a member of KryoSerializer, which would make KryoSerializer not Serializable.  We make
  // the codec lazy here just b/c in some unit tests, we use a KryoSerializer w/out having
  // the SparkEnv set (note those tests would fail if they tried to serialize avro data).
  private lazy val codec = CompressionCodec.createCodec(SparkEnv.get.conf)

  /**
   * Used to compress Schemas when they are being sent over the wire.
   * The compression results are memoized to reduce the compression time since the
   * same schema is compressed many times over
   */
  def compress(schema: StructType): Array[Byte] = compressCache.getOrElseUpdate(schema, {
    val bos = new ByteArrayOutputStream()
    val out = codec.compressedOutputStream(bos)
    Utils.tryWithSafeFinally {
      out.write(schema.json.getBytes(StandardCharsets.UTF_8))
    } {
      out.close()
    }
    bos.toByteArray
  })

  /**
   * Decompresses the schema into the actual in-memory object. Keeps an internal cache of already
   * seen values so to limit the number of times that decompression has to be done.
   */
  def decompress(schemaBytes: ByteBuffer): StructType = decompressCache.getOrElseUpdate(schemaBytes, {
    val bis = new ByteArrayInputStream(
      schemaBytes.array(),
      schemaBytes.arrayOffset() + schemaBytes.position(),
      schemaBytes.remaining())
    val in = codec.compressedInputStream(bis)
    val bytes = Utils.tryWithSafeFinally {
      IOUtils.toByteArray(in)
    } {
      in.close()
    }
    StructType.fromString(new String(bytes, StandardCharsets.UTF_8))
  })

  /**
   * Serializes a record to the given output stream. It caches a lot of the internal data as
   * to not redo work
   */
  def serializeDatum(datum: HoodieSparkRecord, output: Output): Unit = {
    val schema = datum.getStructType
    if (HoodieInternalRowUtils.containsCompressedSchema(schema)) {
      val fingerprint = HoodieInternalRowUtils.getCachedFingerPrintFromSchema(schema)
      output.writeBoolean(true)
      output.writeLong(fingerprint)
    } else {
      output.writeBoolean(false)
      val compressedSchema = compress(schema)
      output.writeInt(compressedSchema.length)
      output.writeBytes(compressedSchema)
    }

    val record = datum.newInstance().asInstanceOf[HoodieSparkRecord]
    record.setStructType(null)
    val byteStream = new ByteArrayOutputStream()
    val stream = new ObjectOutputStream(byteStream)
    stream.writeObject(record)
    output.writeBytes(byteStream.toByteArray)
    stream.close()
    byteStream.close()
    // We do not need to close output. Because output is shared.
  }

  /**
   * Deserializes generic records into their in-memory form. There is internal
   * state to keep a cache of already seen schemas and datum readers.
   */
  def deserializeDatum(input: Input): HoodieSparkRecord = {
    val schema = {
      if (input.readBoolean()) {
        val fingerprint = input.readLong()
        HoodieInternalRowUtils.getCachedSchemaFromFingerPrint(fingerprint)
      } else {
        val length = input.readInt()
        decompress(ByteBuffer.wrap(input.readBytes(length)))
      }
    }
    // We do not need to close input. Because input is shared.
    val stream = new ObjectInputStream(input)
    val record = stream.readObject().asInstanceOf[HoodieSparkRecord]
    record.setStructType(schema)

    record
  }

  override def write(kryo: Kryo, output: Output, datum: HoodieSparkRecord): Unit =
    serializeDatum(datum, output)

  override def read(kryo: Kryo, input: Input, datumClass: Class[HoodieSparkRecord]): HoodieSparkRecord =
    deserializeDatum(input)
}
