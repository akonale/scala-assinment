package co.thebeat.bigdata.takehomeassignment

import java.sql.Timestamp

import org.locationtech.jts.geom.Polygon

package object entity {
  case class DriverLocation(driver: String, timestamp: java.sql.Timestamp, latitude: Double, longitude: Double)

  case class AugmentedDriverLocation(driver: String, timestamp: Timestamp, id_zone: Option[Long], latitude: Double, longitude: Double)
  case class DriverZoneSession(driver: String, session_created_at: Timestamp, id_zone: Option[Long], var count: Int)

  object DriverLocation {
    val timeStampFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
  }

  case class Zone(zoneId: Long, polygon: Polygon)

  case class LatLong(lat: Double, lng: Double)
  case class RawZone(id_zone: Long, polygon: List[LatLong])
  case class RawZones(zones: List[RawZone])
}
