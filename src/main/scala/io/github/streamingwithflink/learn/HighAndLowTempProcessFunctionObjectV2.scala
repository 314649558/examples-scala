package io.github.streamingwithflink.learn

import io.github.streamingwithflink.learn.util.MinMaxTemp
import io.github.streamingwithflink.util.{SensorReading, SensorSource, SensorTimeAssigner}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Created by Administrator on 2019/9/16.
  */
class AssignWindowEndProcessFunction extends ProcessWindowFunction[(String,Double,Double),MinMaxTemp,String,TimeWindow]{

  override def process(key: String,
                       context: Context,
                       elements: Iterable[(String, Double, Double)],
                       out: Collector[MinMaxTemp]): Unit = {

    val minMax=elements.head

    val windowEnd=context.window.getEnd

    out.collect(MinMaxTemp(key,minMax._2,minMax._3,windowEnd))

  }
}




object HighAndLowTempProcessFunctionObjectV2 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1000L)

    val sensorData: DataStream[SensorReading] = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)


    val minMaxTempPerWindow: DataStream[MinMaxTemp] = sensorData
      .map(r => (r.id, r.temperature, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .reduce(
        (r1:(String,Double,Double),r2:(String,Double,Double))=>{
          (r1._1,r1._2.min(r2._2),r1._3.max(r2._3))
        },
        new AssignWindowEndProcessFunction
      )


    minMaxTempPerWindow.print()

    env.execute("minMaxTempPerWindow")

  }

}
