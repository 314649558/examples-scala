package io.github.streamingwithflink.learn

import java.lang

import io.github.streamingwithflink.util.SensorReading
import org.apache.flink.streaming.api.windowing.evictors.Evictor
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue

/**
  * Created by yuanhailong on 2019/9/17.
  *
  * 自定义驱逐器
  *
  * 驱逐器 一般使用在GlobalWindow上面
  *
  */
class MyEvictors extends Evictor[SensorReading,TimeWindow]{
  //Window Function被触发之前调用
  override def evictBefore(iterable: lang.Iterable[TimestampedValue[SensorReading]],   //本窗口的所有元素
                           i: Int,                                                       //窗口大小
                           w: TimeWindow,                                                //Window Object
                           evictorContext: Evictor.EvictorContext): Unit = {
      while (iterable.iterator().hasNext){
        iterable.iterator().remove()  //通过这种方式可以移除窗口中元素
      }
  }

  //Window Function被触发之后调用
  override def evictAfter(iterable: lang.Iterable[TimestampedValue[SensorReading]], i: Int, w: TimeWindow, evictorContext: Evictor.EvictorContext): Unit = ???
}
