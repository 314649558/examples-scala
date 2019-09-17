package io.github.streamingwithflink.learn

import java.util
import java.util.Collections

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.triggers.{EventTimeTrigger, Trigger}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
  * Created by yuanhailong on 2019/9/17.
  * 自定义一个WindowAssigner
  */
class ThirtySecondsWindows extends WindowAssigner[Object,TimeWindow]{

  val windowSize:Long = 30 * 1000L

  override def assignWindows(o: Object,
                             ts: Long,
                             windowAssignerContext: WindowAssigner.WindowAssignerContext): java.util.Collection[TimeWindow] = {
    val startTime= ts - (ts % windowSize)
    val endTime= startTime+windowSize
    Collections.singletonList(new TimeWindow(startTime,endTime))
  }


  override def isEventTime: Boolean = true

  override def getDefaultTrigger(streamExecutionEnvironment: StreamExecutionEnvironment): Trigger[Object, TimeWindow] = {
    EventTimeTrigger.create()
  }



  override def getWindowSerializer(executionConfig: ExecutionConfig): TypeSerializer[TimeWindow] = {
    new TimeWindow.Serializer
  }
}
