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





class HighAndLowTempProcessFunction extends ProcessWindowFunction[SensorReading,MinMaxTemp,String,TimeWindow]{
  override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[MinMaxTemp]): Unit = {


    val temps=elements.map(_.temperature)

    val windowEnd=context.window.getEnd

    out.collect(MinMaxTemp(key,temps.min,temps.max,windowEnd))

  }
}




object HighAndLowTempProcessFunctionObject {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1000L)

    val sensorData: DataStream[SensorReading] = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)


    val minMaxTempPerWindow: DataStream[MinMaxTemp] = sensorData
      .keyBy(_.id)
      .timeWindow(Time.seconds(10))
      .process(new HighAndLowTempProcessFunction)


    minMaxTempPerWindow.print()

    env.execute("minMaxTempPerWindow")

  }

}
