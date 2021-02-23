package co.thebeat.bigdata.takehomeassignment.geo

import co.thebeat.bigdata.takehomeassignment.entity.DriverLocation
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.locationtech.jts.geom.impl.{CoordinateArraySequence, PackedCoordinateSequence}
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, LinearRing, Point, Polygon, PrecisionModel}
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.util.Try

/**
 * Created on 21/02/2021.
 */
class DriverZoneMapperTest extends  FunSuite with BeforeAndAfter {
  var sparkSession: SparkSession = _
  before {
    sparkSession = SparkSession
      .builder()
      .appName("test")
      .master("local[1]")
      .getOrCreate()
  }

  test("testGetPolygon") {
    val factory = new GeometryFactory()
    val forPoint = new CoordinateArraySequence(Array(new Coordinate(51.5,4.8436111)))
    val point = new Point(forPoint, factory)

    val sequence = new CoordinateArraySequence(
      Array[Coordinate](
        new Coordinate(52.1,4.7),
        new Coordinate(52.1,5.0),
        new Coordinate(51.1,5.0),
        new Coordinate(51.1,4.7),
        new Coordinate(52.1,4.7)
      ),
      2)
    val polygon = new Polygon(new LinearRing(sequence, factory), Array[LinearRing](), factory)
    val bool = point.within(polygon)
    println(bool)
    println(new DriverZoneMapper(sparkSession).initializeZones(""))
  }

  test("testZoneMapper") {
    val spark2 = sparkSession
    import spark2.implicits._

    val driverLocations = Seq(
      new DriverLocation("driver_zone6", new java.sql.Timestamp(System.currentTimeMillis()), -12.15, -76.76),
      new DriverLocation("driver_zone1", new java.sql.Timestamp(System.currentTimeMillis()), -11.81, -77.23),
      new DriverLocation("bcd_nozone", new java.sql.Timestamp(System.currentTimeMillis()), 12.15, 76.76)
    ).toDS()

    val path = DriverZoneMapperTest.super.getClass.getResource("/zones.json").toURI.getPath
    println(path)
    val value = new DriverZoneMapper(sparkSession).mapToZone(driverLocations, path)
    val result = value.get
    assert(result.count() == 2)
    val driverZone6 = result
      .filter(adl => adl.driver == "driver_zone6")
      .collectAsList().get(0)
    val driverZone1 = result
      .filter(adl => adl.driver == "driver_zone1")
      .collectAsList().get(0)
    assert(driverZone1.id_zone.get == 1)
    assert(driverZone6.id_zone.get == 6)
  }

}
