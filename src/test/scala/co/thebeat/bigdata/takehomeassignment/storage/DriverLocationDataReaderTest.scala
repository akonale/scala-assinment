package co.thebeat.bigdata.takehomeassignment.storage

import java.time.LocalDateTime

import co.thebeat.bigdata.takehomeassignment.entity.DriverLocation
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
 * Created on 21/02/2021.
 */
class DriverLocationDataReaderTest extends FunSuite with BeforeAndAfter {
  var sparkSession: SparkSession = _

  before {
    sparkSession = SparkSession
      .builder()
      .appName("test")
      .master("local[1]")
      .getOrCreate()
  }

  test("testRead") {

    val testCsv: String = getClass.getResource("/test_files/driver_location_data.csv").toURI.getPath
    val reader = new DriverLocationDataReader(sparkSession)
    val value = reader.read(testCsv)
    val df = value.get
    val l = df.count()
    // Bad row gets ignored
    assert(l == 4)

    implicit val DriverLocationEncoder: Encoder[DriverLocation] = Encoders.product[DriverLocation]
    val readDriverLocation = df
      .filter(row => row.driver == "c473205b")
      .collectAsList().get(0)

    println(readDriverLocation.timestamp.toLocalDateTime)
    println(LocalDateTime.of(2017, 8, 31, 12, 24, 25))

    assert(readDriverLocation.timestamp.toLocalDateTime equals LocalDateTime.of(2017, 8, 31, 12, 24, 25))
    assert(readDriverLocation.latitude equals -12.106778)
    assert(readDriverLocation.longitude equals -76.998078)
  }

}
