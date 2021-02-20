package co.thebeat.bigdata.takehomeassignment.session

import scala.concurrent.duration.Duration
import scala.util.Try

import org.apache.spark.sql.{Dataset, Row}

trait Sessionizer {
  /**
   * Creates sessions for a key. The session's duration is specified from the `duration` parameter.
   *
   * All records in the session should be between the range
   * [session_first_timestamp, session_first_timestamp + duration).
   *
   * Each row of the result represents the session of a driver and the returning dataset should
   * contain the following columns:
   *
   *   - driver: The unique identifier of the driver.
   *   - session_created_at: The timestamp the session started (the timestamp of the first
   *                         event of a session).
   *   - id_zone: The zone that the driver was most active at (had the most events/rows) during
   *              the session. In case of a tie we will favor the zone with larger ID.
   *   - count: The number of the events/rows that the driver had in the most active zone
   *            during the session.
   *
   * For example,
   * input: driver, timestamp, id_zone, latitude, longitude
   *        A, 2019-03-03T12:11:23.000Z, 1, 12.41, 45.289
   *        A, 2019-03-03T12:12:56.000Z, 2, 12.08, 44.921
   *        B, 2019-03-03T12:14:28.000Z, 2, 12.08, 44.921
   *        A, 2019-03-03T12:15:45.000Z, 1, 12.41, 45.289
   *        B, 2019-03-03T12:21:09.000Z, 1, 12.41, 45.289
   *        A, 2019-03-03T12:25:50.000Z, 2, 12.08, 44.921
   *        A, 2019-03-03T12:32:03.000Z, 2, 12.08, 44.921
   *        A, 2019-03-03T12:35:22.000Z, 1, 12.41, 45.289
   *        A, 2019-03-03T12:41:08.000Z, 2, 12.08, 44.921
   *        A, 2019-03-03T12:51:08.000Z, 1, 12.41, 45.289
   *        C, 2019-03-03T13:00:00.000Z, 3, 12.58, 45.025
   *        C, 2019-03-03T13:01:00.000Z, 3, 12.58, 45.025
   *        C, 2019-03-03T13:02:00.000Z, 4, 12.21, 45.357
   *        C, 2019-03-03T13:03:00.000Z, 4, 12.21, 45.357
   *
   * duration: 10 minutes
   *
   * key: driver
   *
   * The output Dataset[Row] should be:
   *
   * driver, session_created_at, id_zone, count
   * A, 2019-03-03T12:11:23.000Z, 1, 2
   * B, 2019-03-03T12:14:28.000Z, 2, 1
   * A, 2019-03-03T12:25:50.000Z, 2, 2
   * A, 2019-03-03T12:41:08.000Z, 2, 1
   * A, 2019-03-03T12:51:08.000Z, 1, 1
   * C, 2019-03-03T13:00:00.000Z, 4, 2
   *
   * @note Assume that input data set doesn't have any null values.
   *
   * @param input A Dataset[Row] containing (among others) the following columns:
   *              driver: String, timestamp: Timestamp, id_zone: Long
   * @param duration The session's duration.
   * @return A Dataset[Row] of the sessions with columns
   *         (driver: String, session_created_at: Timestamp, id_zone: Long, count: Int).A Failure
   *         should be returned if one of the required columns has wrong schema (missing or wrong
   *         typed columns) or the duration is negative.
   */
  def sessionize(input: Dataset[Row], duration: Duration): Try[Dataset[Row]]
}
