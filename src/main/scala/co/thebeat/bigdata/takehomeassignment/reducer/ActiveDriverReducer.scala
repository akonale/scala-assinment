package co.thebeat.bigdata.takehomeassignment.reducer

import co.thebeat.bigdata.takehomeassignment.entity.DriverZoneSession
import org.apache.spark.sql.catalyst.expressions.Rank
import org.apache.spark.sql.{Dataset, Row, SparkSession, functions}

import scala.util.Try

/**
 * Created on 23/02/2021.
 */
class ActiveDriverReducer(spark: SparkSession) extends Reducer {
  /**
   * Finds the most active driver (based on the count field) for each geographic area. In case of
   * a tie we will favor the larger driver value (String comparison).
   *
   * Each row of the results correspond to one zone (each id_zone of the input will be present only
   * once in the output), along with information about the session and driver with the highest
   * score in each zone. Put another way, the method returns for each id_zone,
   * the input row corresponding to the max(count) of the zone and in case of tie the max(driver).
   * This is not 100% true, as at the very end we need to keep only the columns we are interested
   * in. Specifically, the output should contain only the following columns:
   *
   * id_zone: The id of the zone that we want to find the best (more active) driver.
   * driver: The driver with the highest score (count field) in this zone.
   * session_created_at: The session creation time corresponding to the highest score.
   * count: The number of the events that the driver had in this specific zone.
   *
   * The result should contain only one row for each zone with the highest score.
   *
   * For example:
   * input: driver, session_created_at, id_zone, count
   * A, 12:08:15, 1, 2
   * B, 12:10:34, 1, 2
   * A, 12:28:02, 2, 5
   * B, 12:31:51, 2, 4
   * B, 12:47:04, 2, 2
   * B, 13:00:29, 2, 3
   *
   * output: id_zone, driver, session_created_at, count
   * 1, B, 12:10:34, 2
   * 2, A, 12:28:02, 5
   *
   * @note Assume that input data does not contain null values because they are filtered out in previous steps.
   * @param input : A Dataset[Row] with schema (among others):
   *              driver: String, session_created_at: Timestamp, id_zone: Long, count: Int
   * @return A Dataset[Row] with the most active driver for every zone, with columns:
   *         id_zone: Long, driver: String, session_created_at: Timestamp, count: Int.
   *         A Failure should be returned if the input data doesn't have the correct schema (missing
   *         or wrong typed columns).
   *
   */
  override def reduce(input: Dataset[DriverZoneSession]): Try[Dataset[DriverZoneSession]] = {
    import spark.implicits._
    import org.apache.spark.sql.expressions.Window
    val window = Window
      .partitionBy("id_zone")
      .orderBy($"count".desc, $"driver".desc)

    val reducedSessions = input
      .withColumn("rank", functions.rank().over(window).as("rank"))
        .filter($"rank".equalTo(1))
        .drop("rank")
        .as[DriverZoneSession]

    Try(reducedSessions)
  }
}
