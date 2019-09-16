package io.github.streamingwithflink.learn

import org.apache.flink.api.common.functions.AggregateFunction

/**
  * Created by Administrator on 2019/9/16.
  */
class AvgTempFunction extends AggregateFunction[(String,Double),(String,Double,Int),(String,Double)]{


  override def createAccumulator(): (String, Double, Int) = {
    ("",0.0,0)
  }


  override def add(value: (String, Double), accumulator: (String, Double, Int)): (String, Double, Int) = {
    (value._1,value._2+accumulator._2,1+accumulator._3)
  }



  override def getResult(accumulator: (String, Double, Int)): (String, Double) = {
    (accumulator._1,accumulator._2/accumulator._3)
  }

  override def merge(a: (String, Double, Int), b: (String, Double, Int)): (String, Double, Int) = {
    (a._1,a._2+b._2,a._3+b._3)
  }
}
