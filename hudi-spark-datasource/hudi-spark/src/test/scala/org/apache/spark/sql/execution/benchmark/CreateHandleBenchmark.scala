package org.apache.spark.sql.execution.benchmark

import org.apache.avro.generic.IndexedRecord
import org.apache.hudi.client.WriteStatus
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.data.HoodieData
import org.apache.hudi.common.engine.{HoodieLocalEngineContext, LocalTaskContextSupplier}
import org.apache.hudi.common.model.{DefaultHoodieRecordPayload, HoodieAvroIndexedRecord, HoodieKey, HoodieRecord}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.table.marker.MarkerType
import org.apache.hudi.common.util.HoodieRecordUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.io.HoodieCreateHandle
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
import org.apache.hudi.table.{HoodieSparkTable, HoodieTable}
import org.apache.hudi.{AvroConversionUtils, HoodieSparkUtils}
import org.apache.spark.hudi.benchmark.{HoodieBenchmark, HoodieBenchmarkBase}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import java.util.stream.Collectors
import java.util.{Properties, UUID}
import scala.util.Random

object CreateHandleBenchmark extends HoodieBenchmarkBase {
  protected val spark: SparkSession = getSparkSession

  def getSparkSession: SparkSession = SparkSession
    .builder()
    .master("local[1]")
    .config("spark.driver.memory", "8G")
    .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    .appName(this.getClass.getCanonicalName)
    .getOrCreate()

  def getDataFrame(numbers: Int): DataFrame = {
    val rand = new Random(42)
    val schema = createRandomSchema(numCols = 100, maxDepth = 5)
    val rows = (1 to numbers).map(_ => generateRow(schema, rand))
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
  }

  def createRandomSchema(numCols: Int, maxDepth: Int): StructType = {
    val types = Seq("string", "long", "int", "array", "map", "struct")
    val fields = (1 to numCols).map { i =>
      val dataType = types((i - 1) % types.length)
      val colName = s"col$i"

      val fieldType = dataType match {
        case "string" => StringType
        case "long" => LongType
        case "int" => IntegerType
        case "array" => ArrayType(StringType, containsNull = false)
        case "map" => MapType(StringType, IntegerType, valueContainsNull = false)
        case "struct" => generateNestedStruct(maxDepth)
      }

      StructField(colName, fieldType, nullable = false)
    }

    StructType(StructField("key", StringType, nullable = false) +: fields)
  }

  def generateNestedStruct(depth: Int): StructType = {
    if (depth <= 0) {
      StructType(Seq(
        StructField("leafStr", StringType, nullable = false),
        StructField("leafInt", IntegerType, nullable = false)
      ))
    } else {
      StructType(Seq(
        StructField("nestedStr", StringType, nullable = false),
        StructField("nestedInt", IntegerType, nullable = false),
        StructField("nestedStruct", generateNestedStruct(depth - 1), nullable = false)
      ))
    }
  }

  def generateRow(schema: StructType, rand: Random): Row = {
    val values = schema.fields.map {
      case StructField("key", _, _, _) => java.util.UUID.randomUUID().toString
      case StructField(_, StringType, _, _) => s"str_${rand.nextInt(100)}"
      case StructField(_, LongType, _, _) => rand.nextLong()
      case StructField(_, IntegerType, _, _) => rand.nextInt(100)
      case StructField(_, ArrayType(_, _), _, _) => Seq.fill(3)(s"arr_${rand.nextInt(100)}")
      case StructField(_, MapType(_, _, _), _, _) => Map("a" -> rand.nextInt(10), "b" -> rand.nextInt(10))
      case StructField(_, s: StructType, _, _) => generateRow(s, rand)
      case _ => throw new RuntimeException("Unsupported type")
    }
    Row.fromSeq(values)
  }

  private def createHandleBenchmark: Unit = {
    val benchmark = new HoodieBenchmark(s"perf create handle for hoodie", 10000)
    val df = getDataFrame(100000)
    val avroSchema = AvroConversionUtils.convertStructTypeToAvroSchema(df.schema, "record", "my")
    spark.sparkContext.getConf.registerAvroSchemas(avroSchema)

    df.write.format("hudi").option(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "key")
      .option(HoodieMetadataConfig.ENABLE.key(),"false")
      .option(HoodieTableConfig.NAME.key(), "tbl_name").mode(SaveMode.Overwrite).save("/tmp/sample_test_table")
    val dummpProps = new Properties()
    val avroRecords: java.util.List[HoodieRecord[_]] = HoodieSparkUtils.createRdd(df, "struct_name", "name_space",
      Some(avroSchema)).mapPartitions(
      it => {
        it.map { genRec =>
          val hoodieKey = new HoodieKey(genRec.get("key").toString, "")
          HoodieRecordUtils.createHoodieRecord(genRec, 0L, hoodieKey, classOf[DefaultHoodieRecordPayload].getName, false)
        }
      }).toJavaRDD().collect().stream().map[HoodieRecord[_]](hoodieRec => {
      hoodieRec.asInstanceOf[HoodieAvroIndexedRecord].toIndexedRecord(avroSchema, dummpProps)
      hoodieRec
    }).collect(Collectors.toList[HoodieRecord[_]])

    benchmark.addCase("create handle perf benchmark") { _ =>
      val props = new Properties()
      props.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "key")
      val writeConfig = HoodieWriteConfig.newBuilder().withPath("/tmp/sample_test_table").withPreCombineField("col1")
        .withSchema(avroSchema.toString)
        .withMarkersType(MarkerType.DIRECT.name())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .withProps(props).build()

      val engineContext = new HoodieLocalEngineContext(new HadoopStorageConfiguration(spark.sparkContext.hadoopConfiguration))
      val hoodieTable: HoodieTable[_, HoodieData[HoodieRecord[_]], HoodieData[HoodieKey], HoodieData[WriteStatus]] =
        HoodieSparkTable.create(writeConfig, engineContext).asInstanceOf[HoodieTable[_, HoodieData[HoodieRecord[_]], HoodieData[HoodieKey], HoodieData[WriteStatus]]]
      val createHandle = new HoodieCreateHandle(writeConfig, "000000001", hoodieTable, "", UUID.randomUUID().toString, new LocalTaskContextSupplier())
      avroRecords.forEach(record => {
        val newAvroRec = new HoodieAvroIndexedRecord(record.getKey, record.getData.asInstanceOf[IndexedRecord], 0L, record.getOperation)
        createHandle.write(newAvroRec, avroSchema, writeConfig.getProps)
      })
      createHandle.close()
    }
    benchmark.run()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    createHandleBenchmark
  }
}
