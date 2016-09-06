package summercamp.flink

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.log4j.Logger

import scala.util.Random

class SensorDataSource(val sensorType: SensorType, val numberOfSensors: Int,
                       val watermarkTag: Int, val numberOfElements: Int = -1) extends SourceFunction[SensorData] {
  final val serialVersionUID = 1L
  @volatile var isRunning = true
  var counter = 1
  var timestamp = 0
  val randomGen = Random

  require(numberOfSensors > 0)
  require(numberOfElements >= -1)

  lazy val initialReading: Double = {
    sensorType match {
      case TemperatureSensor => 27.0
      case HumiditySensor => 0.75
    }
  }

  override def run(ctx: SourceContext[SensorData]): Unit = {

    val counterCondition = {
      if(numberOfElements == -1) {
         x: Int => isRunning
      } else {
        x: Int => isRunning && counter <= x
      }
    }

    while (counterCondition(numberOfElements)) {

      Thread.sleep(10) // send sensor data every 10 milliseconds

      val dataId = randomGen.nextInt(numberOfSensors) + 1

      val data = SensorData(dataId.toString, initialReading + Random.nextGaussian()/initialReading, sensorType, timestamp)

      ctx.collectWithTimestamp(data, timestamp)

      timestamp = timestamp + 1

      if (timestamp % watermarkTag == 0) {
        ctx.emitWatermark(new Watermark(timestamp))  // watermark in milliseconds
      }
      counter = counter + 1
    }
  }

  override def cancel(): Unit = {
    // No cleanup needed
    isRunning = false
  }
}
