package summercamp.flink

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.log4j.Logger

object SensorSimple {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // set default env parallelism for all operators
    env.setParallelism(2)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val numberOfSensors = 2
    val watermarkTag = 10
    val numberOfElements = 100

    val sensorDataStream =
      env.addSource(new SensorDataSource(TemperatureSensor, numberOfSensors, watermarkTag, numberOfElements))

    sensorDataStream.writeAsText("inputData.txt")

    val windowedKeyed = sensorDataStream
      .keyBy(data => data.sensorId)
      .timeWindow(Time.milliseconds(10))

    windowedKeyed.max("value")
      .writeAsText("outputMaxValue.txt")

    windowedKeyed.apply(new SensorAverage())
      .writeAsText("outputAverage.txt")

    env.execute("Sensor Data Simple Statistics")
  }
}

class SensorAverage extends WindowFunction[SensorData, SensorData, String, TimeWindow] {
  def apply(key: String, window: TimeWindow, input: Iterable[SensorData], out: Collector[SensorData]): Unit = {
   Logger.getRootLogger.info(s"data:${input.mkString(",")}" + s"size:${input.size}")
    if (input.nonEmpty) {
      val average = input.map(_.value).sum / input.size
      out.collect(input.head.copy(value = average))
    }
  }
}
