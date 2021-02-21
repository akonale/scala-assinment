package co.thebeat.bigdata.takehomeassignment.geo

import org.locationtech.jts.geom.impl.{CoordinateArraySequence, PackedCoordinateSequence}
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, LinearRing, Point, Polygon}
import org.scalatest.FunSuite

import scala.util.Try

/**
 * Created on 21/02/2021.
 */
class DriverZoneMapperTest extends FunSuite {

  test("testMapToZone") {
    val doubles: Array[Double] = Array[Double](2.11, 3.22)

    val point = new Point(new PackedCoordinateSequence.Double(doubles, 2), new GeometryFactory())
    val sequence = new CoordinateArraySequence(Array[Coordinate](), 2)
    val polygon = new Polygon(new LinearRing(sequence, new GeometryFactory()), Array[LinearRing](), new GeometryFactory())
    val bool = point.within(polygon)
    bool
  }

}
