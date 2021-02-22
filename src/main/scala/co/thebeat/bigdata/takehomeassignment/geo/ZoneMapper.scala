package co.thebeat.bigdata.takehomeassignment.geo

import co.thebeat.bigdata.takehomeassignment.entity.{AugmentedDriverLocation, DriverLocation}
import org.apache.spark.sql.{Dataset, Row}

import scala.util.Try

trait ZoneMapper {
  /**
   * Maps each row to the geographical area or geographical zone it belongs to.
   * If a row does not belong to any zone, it should be filtered out. We consider points that belong exactly to the boundary line
   * of a zone to be considered as out-of-zone point. The returned data should be augmented by the non-nullable id_zone column,
   * which specifies the zone a Row belongs to.
   *
   * For example,
   * input: id, latitude, longitude, timestamp
   *        1, 12.34, 45.31, 2019-03-02 00:00:00
   *        2, 12.53, 44.99, 2019-03-03 00:00:00
   *        3, 12.19, 45,03, 2019-03-04 00:00:00
   *
   * Assuming that point (12.34, 45.31) lies inside zone 100, point (12.53, 44.99) is an out-of-zone point
   * and point (12.19, 45,03) lies inside zone 200, the output would be
   * output:  id, latitude, longitude, timestamp, id_zone
   *          1, 12.34, 45.31, 2019-03-02 00:00:00, 100
   *          3, 12.19, 45,03, 2019-03-04 00:00:00, 200
   *
   * Information about zones is provided by the src/main/resources/zones.json file.
   *
   * @note Assume that input data does not contain null values.
   *
   * @param input : the input must contain a column named latitude of type Double,
   *              and a column longitude of type Double.
   *              If the input schema does not match the requirements a Failure must be returned.
   * @param path  : String, the path to a JSON file holding zone related information.
   *              The content of the file should be the similar to the following:
   *              {
   *                "zones":[
   *                  {
   *                    "id_zone":1,
   *                    "polygon":[
   *                      {"lat":-11.784234676000215,"lng":-77.28648789023627},
   *                      {"lat":-11.873627286463377,"lng":-77.28648789023627},
   *                      {"lat":-11.869434808611365,"lng":-77.19452474207226},
   *                      {"lat":-11.780071954813682,"lng":-77.19452474207226},
   *                      ...
   *                      {"lat":-11.784234676000215,"lng":-77.28648789023627}
   *                    ]
   *                  },
   *                  {
   *                    "id_zone":2,
   *                    "polygon":[
   *                      {"lat":-11.775876340179618,"lng":-77.10249019404858},
   *                      {"lat":-11.865209200247359,"lng":-77.10249019404858},
   *                      ...
   *                      {"lat":-11.775876340179618,"lng":-77.10249019404858}
   *                    ]
   *                  },
   *                  ...
   *                ]
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
  def mapToZone(input: Dataset[DriverLocation], path: String): Try[Dataset[AugmentedDriverLocation]]
}
