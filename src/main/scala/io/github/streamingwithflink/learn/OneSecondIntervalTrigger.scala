package io.github.streamingwithflink.learn

import io.github.streamingwithflink.util.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
  * Created by yuanhailong on 2019/9/17.
  *
  * 自定义触发器
  */
class OneSecondIntervalTrigger extends Trigger[SensorReading,TimeWindow]{

  override def onEventTime(timestamp: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {

    if(timestamp==w.getEnd){
      TriggerResult.FIRE_AND_PURGE  //如果到达了窗口的尾端则触发窗口计算，并清空Window状态，销毁Window Object
    }else{
      val t=triggerContext.getCurrentWatermark + (1000-triggerContext.getCurrentWatermark%1000)
      if(t<w.getEnd){
        triggerContext.registerEventTimeTimer(t)
      }
      TriggerResult.FIRE  //仅触发窗口计算，不清除状态
    }
  }

  override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE   //什么都不做，继续执行(相当于什么都没干)
  }

  override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {
    val firstSeen:ValueState[Boolean]=triggerContext.getPartitionedState(
      new ValueStateDescriptor[Boolean]("firstSeen",classOf[Boolean])
    )
    firstSeen.clear()   //对状态进行清空

  }

  override def onElement(t: SensorReading,
                         l: Long,
                         w: TimeWindow,
                         triggerContext: Trigger.TriggerContext): TriggerResult = {

    //如果firstSeen 没有被设置那么返回false
    val firstSeen:ValueState[Boolean] = triggerContext.getPartitionedState(
      new ValueStateDescriptor[Boolean]("firstSeen",classOf[Boolean])
    )
    //为第一个元素注册初始Timer
    if(!firstSeen.value()){
      val t= triggerContext.getCurrentWatermark + (1000 - triggerContext.getCurrentWatermark % 1000)
      triggerContext.registerEventTimeTimer(t)
      //注册window end
      triggerContext.registerEventTimeTimer(w.getEnd)
      firstSeen.update(true)
    }
    TriggerResult.CONTINUE



  }
}
