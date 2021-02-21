package co.thebeat.bigdata.takehomeassignment.storage

import co.thebeat.bigdata.takehomeassignment.entity.DriverLocation

import scala.util.Try
import org.apache.spark.sql.{Dataset, Row}

trait Reader {
  /**
   * Reads the data which are located in the specified path, returning the result as a
   * `Dataset[Row]`.
   * The path could be either a local or distributed file system.
   *
   * Only one `Dataset[Row]` should be returned, even if data are partitioned in many files.
   *
   * Moreover malformed rows (rows that don't conform to the specified schema) and rows with at
   * least one `null` value should be filtered out.
   *
   * e.g. Let the schema of the data be `[String, Timestamp, Int]`. The data are stored in a CSV
   *      file.
   *
   *      Assume the data are:
   *         id,timestamp,in_ride
   *         driver1,2019-03-03T03:00:00.000Z,1
   *         driver2,error,1
   *         driver3,,0
   *         4,2017-12-01T07:00:00.000Z,0
   *
   *     Only rows 1 and 4 should be returned as row 2 doesn't conform with the schema (there is
   *     no `Timestamp` and row 3 has a null value.
   *
   * @param path Location of files (could be a local or distributed file system). To read
   *             partitioned files specify a directory. In case the provided path does not exist, a
   *             Failure should be returned.
   * @return Loads the data and filters out malformed rows and rows with null values, returning the
   *         result as a `Dataset[Row]`.
   */
  def read(path: String): Try[Dataset[DriverLocation]]
}
