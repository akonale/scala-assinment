package co.thebeat.bigdata.takehomeassignment.geo

import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.impl.{CoordinateArraySequence, PackedCoordinateSequence}
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, LinearRing, Point, Polygon, PrecisionModel}
import org.scalatest.FunSuite

import scala.util.Try

/**
 * Created on 21/02/2021.
 */
class DriverZoneMapperTest extends FunSuite {

  test("testMapToZone") {
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

    val sparkSession = SparkSession
      .builder()
      .appName("test")
      .master("local[1]")
      .getOrCreate()

    println(new DriverZoneMapper(sparkSession).polygons)
  }

}
