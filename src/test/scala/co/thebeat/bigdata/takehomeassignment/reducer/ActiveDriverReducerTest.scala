package co.thebeat.bigdata.takehomeassignment.reducer

import co.thebeat.bigdata.takehomeassignment.entity.{AugmentedDriverLocation, DriverLocation, DriverZoneSession}
import co.thebeat.bigdata.takehomeassignment.session.DriverZoneSessionizer
import co.thebeat.bigdata.takehomeassignment.storage.DriverZoneSessionWriter
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
 * Created on 23/02/2021.
 */
class ActiveDriverReducerTest extends FunSuite with BeforeAndAfter {

  var sparkSession: SparkSession = _
  before {
    sparkSession = SparkSession
      .builder()
      .appName("test")
      .master("local[1]")
      .getOrCreate()
  }

  test("testReduce") {
    val testCsv: String = getClass.getResource("/test_files/driver_zone_sessions.csv").toURI.getPath
    val spark = sparkSession
    import spark.implicits._

    val schema = ScalaReflection.schemaFor[DriverZoneSession].dataType.asInstanceOf[StructType]

    val df: Dataset[DriverZoneSession] = spark
      .read
      .option("header", value = true)
      .option("timestampFormat", DriverLocation.timeStampFormat)
      .option("mode", "DROPMALFORMED")
      .schema(schema)
      .csv(testCsv)
      .as[DriverZoneSession]

    val value = new ActiveDriverReducer(sparkSession).reduce(df)
    value.get.show()
  }

}
