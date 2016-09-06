package summercamp.flink

sealed trait SensorType { def stype: String }
case object TemperatureSensor extends SensorType { val stype = "TEMP" }
case object HumiditySensor extends SensorType { val stype = "HUM" }

case class SensorData(var sensorId: String, var value: Double, var sensorType: SensorType, timestamp: Long)
