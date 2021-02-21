package co.thebeat.bigdata.takehomeassignment

package object entity {
  case class DriverLocation(driver: String, timestamp: java.sql.Timestamp, latitude: Double, longitude: Double)

  object DriverLocation {
    val timeStampFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
  }

}
