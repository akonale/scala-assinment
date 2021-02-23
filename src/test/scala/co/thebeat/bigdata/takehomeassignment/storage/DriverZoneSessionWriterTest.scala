package co.thebeat.bigdata.takehomeassignment.storage

import java.io.File
import java.nio.file.{Files, Path}

import co.thebeat.bigdata.takehomeassignment.entity.DriverZoneSession
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
 * Created on 23/02/2021.
 */
class DriverZoneSessionWriterTest extends FunSuite with BeforeAndAfter {

  var sparkSession: SparkSession = _
  var tempDir: File = _
  before {
    sparkSession = SparkSession
      .builder()
      .appName("test")
      .master("local[1]")
      .getOrCreate()

    tempDir = Files.createTempDirectory("spark").toFile
  }
  after {
    val files = tempDir.listFiles()
    for(each <- files){
      each.delete()
    }
    tempDir.delete()
  }

  test("testWrite") {
    val spark = sparkSession
    import spark.implicits._

    val input1 = Seq(
      DriverZoneSession("A", new java.sql.Timestamp(System.currentTimeMillis()), Some(1), 1),
      DriverZoneSession("B", new java.sql.Timestamp(System.currentTimeMillis()), Some(2), 1)
    ).toDS()

    val input2 = Seq(
      DriverZoneSession("C", new java.sql.Timestamp(System.currentTimeMillis()), Some(3), 1),
      DriverZoneSession("D", new java.sql.Timestamp(System.currentTimeMillis()), Some(4), 1)
    ).toDS()

    println(s"Writing results to $tempDir")
    val writer = new DriverZoneSessionWriter(sparkSession)
    writer.write(input1, tempDir.getAbsolutePath)
    writer.write(input2, tempDir.getAbsolutePath)

    val frame = sparkSession.read.csv(tempDir.getAbsolutePath)
    // to files, 2 headers + 4 records
    assert(frame.count() == 6)
  }

}
