package co.thebeat.bigdata.takehomeassignment.geo

import java.io.FileInputStream

import co.thebeat.bigdata.takehomeassignment.entity.{AugmentedDriverLocation, DriverLocation, RawZones, Zone}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.locationtech.jts.geom.impl.CoordinateArraySequence
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, LinearRing, Point, Polygon}

import scala.io.Source
import scala.util.{Failure, Success, Try}

class DriverZoneMapper(spark: SparkSession) extends ZoneMapper {

  def initializeZones(path: String): List[Zone] = {
    val is = new FileInputStream(path)
    val jsonString = Source.fromInputStream(is).mkString("")

    implicit val formats: DefaultFormats.type = DefaultFormats

    val rawZones = parse(jsonString).extract[RawZones]
    rawZones.zones.map(
      rawZone => {
        val coords: Array[Coordinate] = rawZone.polygon.map(
          latlng => new Coordinate(latlng.lat, latlng.lng)
        ).toArray
        val sequence = new CoordinateArraySequence(coords, 2)
        val factory = new GeometryFactory()

        val polygon = new Polygon(new LinearRing(sequence, factory), Array[LinearRing](), factory)
        Zone(rawZone.id_zone, polygon)
      }
    )

  }

  /**
   * Maps each row to the geographical area or geographical zone it belongs to.
   * If a row does not belong to any zone, it should be filtered out. We consider points that belong exactly to the boundary line
   * of a zone to be considered as out-of-zone point. The returned data should be augmented by the non-nullable id_zone column,
   * which specifies the zone a Row belongs to.
   *
   * For example,
   * input: id, latitude, longitude, timestamp
   * 1, 12.34, 45.31, 2019-03-02 00:00:00
   * 2, 12.53, 44.99, 2019-03-03 00:00:00
   * 3, 12.19, 45,03, 2019-03-04 00:00:00
   *
   * Assuming that point (12.34, 45.31) lies inside zone 100, point (12.53, 44.99) is an out-of-zone point
   * and point (12.19, 45,03) lies inside zone 200, the output would be
   * output:  id, latitude, longitude, timestamp, id_zone
   * 1, 12.34, 45.31, 2019-03-02 00:00:00, 100
   * 3, 12.19, 45,03, 2019-03-04 00:00:00, 200
   *
   * Information about zones is provided by the src/main/resources/zones.json file.
   *
   * @note Assume that input data does not contain null values.
   * @param input : the input must contain a column named latitude of type Double,
   *              and a column longitude of type Double.
   *              If the input schema does not match the requirements a Failure must be returned.
   * @param path  : String, the path to a JSON file holding zone related information.
   *              The content of the file should be the similar to the following:
   *              {
   *              "zones":[
   *              {
   *              "id_zone":1,
   *              "polygon":[
   *              {"lat":-11.784234676000215,"lng":-77.28648789023627},
   *              {"lat":-11.873627286463377,"lng":-77.28648789023627},
   *              {"lat":-11.869434808611365,"lng":-77.19452474207226},
   *              {"lat":-11.780071954813682,"lng":-77.19452474207226},
   *              ...
   *              {"lat":-11.784234676000215,"lng":-77.28648789023627}
   *              ]
   *              },
   *              {
   *              "id_zone":2,
   *              "polygon":[
   *              {"lat":-11.775876340179618,"lng":-77.10249019404858},
   *              {"lat":-11.865209200247359,"lng":-77.10249019404858},
   *              ...
   *              {"lat":-11.775876340179618,"lng":-77.10249019404858}
   *              ]
   *              },
   *              ...
   *              ]
   *              }
   *
   *              The file contains a list of zones. Each zone is represented by a unique identifier (of type Long)
   *              and a polygon. A polygon is simply a counterclockwise sequence of latitude and longitude pairs,
   *              with the first pair matching the last one (otherwise a failure must be returned).
   *              In case of malformed JSON a Failure must be returned.
   *              The same must happen in case the provided path to the JSON file does not exist.
   * @return A Dataset[Row] containing a subset (maybe empty) of the rows of the input.
   *         The resulting Dataset must contain one additional column id_zone of type Long which cannot contain null values.
   */
  override def mapToZone(input: Dataset[DriverLocation], path: String): Try[Dataset[AugmentedDriverLocation]] = {
    val triedFrame = Try {
      val polygons: List[Zone] = initializeZones(path)
      val factory = new GeometryFactory()
      import spark.implicits._
      val adls = input
        .map(dl => {
          val point = new Point(new CoordinateArraySequence(Array(new Coordinate(dl.latitude, dl.longitude))), factory)
          val maybeZone = polygons
            .find(zone => zone.polygon.contains(point))
          val zoneId = if (maybeZone.isDefined) Some(maybeZone.get.zoneId) else None
          AugmentedDriverLocation(dl.driver, dl.timestamp, dl.latitude, dl.longitude, zoneId)
        })
        .filter(adl => adl.zoneId.isDefined)
      adls
    }
    triedFrame match {
      case Success(value) => Success(value)
      case Failure(exception) => Failure(exception)
    }
  }
}
