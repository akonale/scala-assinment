package co.thebeat.bigdata.takehomeassignment.storage

import scala.util.Try

import org.apache.spark.sql.{Dataset, Row}

trait Writer {
  /**
   * Tries to write the input Dataset[Row] to the specified path. The path could be either a local
   * or distributed file system.
   *
   * If data already exists in the path, contents of the input Dataset[Row] are expected to be
   * appended to the data that already exists. So if we try to read data from the path we will get
   * back both, old and new data.
   *
   * If the data cannot be written (e.g. when trying to write a Map or List value to a CSV file) a
   * Failure should be returned.
   *
   * @param input The Dataset[Row] that will be saved
   * @param path The location where the input Dataset[Row] will be saved
   * @return Success if the Dataset[Row] was saved successfully, Failure otherwise
   */
  def write(input: Dataset[Row], path: String): Try[Unit]
}
