package co.thebeat.bigdata.takehomeassignment

import co.thebeat.bigdata.takehomeassignment.geo.ZoneMapper
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import co.thebeat.bigdata.takehomeassignment.storage.{DriverDataReader, Reader, Writer}
import co.thebeat.bigdata.takehomeassignment.session.Sessionizer
import co.thebeat.bigdata.takehomeassignment.reducer.Reducer

import scala.util.Try


object Assignment extends App {
  // Use the SparkSession in any part of the assignment you need one.
  def spark: SparkSession = SparkSession
    .builder()
    .appName("take-home-assignment")
    .master("local[1]")
    .getOrCreate()

  // Should read all data from the provided CSV files and return a Dataset[Row] with schema
  // [String, Timestamp, Double, Double]. Rows with malformed data or rows with null values must be
  // filtered out.
  //
  // The first row in the CSV is the header.
  //
  // @Note: Method `read` must follow all constraints that the `Reader` trait has set.
  lazy val reader: Reader = new DriverDataReader(spark)

  // Should save a Dataset[Row] as a CSV file. The first line of the CSV should be the header.
  //
  // @Note: Method `write` must follow all constraints that the `Writer` trait has set.
  lazy val writer: Writer = ???

  // In case of success it should return a Dataset[Row] with columns
  // (driver: String, session_created_at: Timestamp, id_zone: Long, count: Int).
  // Sessions should be created based on the driver ID. If an input Dataset[Row] without the needed
  // columns is passed as input, or the duration is negative a failure should be returned.
  //
  // @Note: Method `sessionize` must follow all constraints that the `Sessionizer` trait has set.
  lazy val sessionizer: Sessionizer = ???

  // In case of success it should return a Dataset[Row] with columns
  // (id_zone: Long, driver: String, session_created_at: Timestamp, count: Int)
  // A Failure should be returned if the input data doesn't have the correct schema.
  //
  // @Note: Method `reduce` must follow all constraints that the `Reducer` trait has set.
  lazy val reducer: Reducer = ???

  // Should map all rows to geographical zones (an abstraction containing an identifier and a polygon),
  // filtering out rows that do not belong to any zone.
  // For rows that map to more than one zone the class will map to any of them (randomly).
  //
  // @Note: Method `mapToZone` must follow all constraints that the `ZoneMapper` trait has set.
  lazy val zoneMapper: ZoneMapper = ???

  // You need to assemble the different components of the assignment, so to be able to run the pre-processing pipeline.
  def runPipeline(): scala.util.Try[Unit] = {
    val value: Try[Dataset[Row]] = reader.read("/Users/akonale/IdeaProjects/beat/bigdata-challenge/assignment-project/src/main/resources/test_data.csv")
    Try(value.get.show())
  }

  runPipeline() match {
    case scala.util.Success(_) => println("Kudos! Everything executed successfully")
    case scala.util.Failure(exception) => println(s"Something went wrong! Message: ${exception.getMessage}")
  }
}
