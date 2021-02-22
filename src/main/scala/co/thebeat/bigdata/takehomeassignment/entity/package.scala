package co.thebeat.bigdata.takehomeassignment

import org.locationtech.jts.geom.Polygon

package object entity {
  case class DriverLocation(driver: String, timestamp: java.sql.Timestamp, latitude: Double, longitude: Double)
  case class AugmentedDriverLocation(driver: String, timestamp: java.sql.Timestamp, latitude: Double, longitude: Double, zoneId: Option[Int])

  object DriverLocation {
    val timeStampFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
  }

  case class Zone(zoneId: Int, polygon: Polygon)

  case class LatLong(lat: Double, lng: Double)
  case class RawZone(id_zone: Int, polygon: List[LatLong])
  case class RawZones(zones: List[RawZone])
}
