package co.thebeat.bigdata.takehomeassignment.session

import java.util.concurrent.TimeUnit

import co.thebeat.bigdata.takehomeassignment.entity.{AugmentedDriverLocation, DriverLocation, DriverZoneSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{Duration, FiniteDuration}

/**
 * Created on 23/02/2021.
 */
class DriverZoneSessionizerTest extends FunSuite with BeforeAndAfter {
  var sparkSession: SparkSession = _
  before {
    sparkSession = SparkSession
      .builder()
      .appName("test")
      .master("local[1]")
      .getOrCreate()
  }

  test("testSessionize") {
    val testCsv: String = getClass.getResource("/test_files/augumented_driver_location.csv").toURI.getPath
    val spark = sparkSession
    import spark.implicits._

    val schema = ScalaReflection.schemaFor[AugmentedDriverLocation].dataType.asInstanceOf[StructType]

    val df: Dataset[AugmentedDriverLocation] = spark
      .read
      .option("header", value = true)
      .option("timestampFormat", DriverLocation.timeStampFormat)
      .option("mode", "DROPMALFORMED")
      .schema(schema)
      .csv(testCsv)
      .as[AugmentedDriverLocation]

    val sessionDuration = new FiniteDuration(10, TimeUnit.MINUTES)
    val driverZones = new DriverZoneSessionizer(sparkSession)
      .sessionize(df, sessionDuration)
    driverZones.get.show()
    val driverASessions = driverZones.get.filter(adl => adl.driver == "A" && adl.id_zone.get == 2).collect()
    assert(driverASessions.length == 3)
    val sessionStartTimes = driverASessions.map(dz => dz.session_created_at.getTime).sortBy(t => t)
    val iterator = sessionStartTimes.iterator
    var first: Long = iterator.next()
    var second: Long = 0L
    while (iterator.hasNext){
      second = iterator.next()
      val diff = second - first
      assert(diff > sessionDuration.toMillis)
      first = second
    }

  }

}
