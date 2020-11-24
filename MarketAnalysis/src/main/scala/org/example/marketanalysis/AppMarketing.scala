package org.example.marketanalysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.example.marketanalysis.AppMarketingByChannel.{SimulatedEvenSource}


object AppMarketing {
  def main(args: Array[String]): Unit = {
    // create execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism( 1 )
    env.setStreamTimeCharacteristic( TimeCharacteristic.EventTime )

    val dataStream = env.addSource( new SimulatedEvenSource() )
      .assignAscendingTimestamps( _.timestamp )
      .filter( _.behavior != "UNINSTALL" )
      .map( _ => ("dummyKey", 1L) )
      .keyBy( _._1 )
      .timeWindow( Time.hours( 1 ), Time.seconds( 10 ) )
      .aggregate( new CountAgg(), new MarketingCountTotal() )

    dataStream.print( "market total count" )
    env.execute( "market job" )
  }
}

  class CountAgg() extends AggregateFunction[(String, Long), Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: (String, Long), acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class MarketingCountTotal() extends WindowFunction[Long, MarketingViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[MarketingViewCount]): Unit = {
    val startTs = new Timestamp( window.getStart ).toString
    val endTs = new Timestamp( window.getEnd ).toString
    out.collect( MarketingViewCount( startTs, endTs, "app market", "total", input.iterator.next ) )
  }
}
