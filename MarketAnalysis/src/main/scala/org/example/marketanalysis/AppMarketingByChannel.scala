package org.example.marketanalysis

import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

case class MartketingUserBehavior (userId: String, behavior: String, channel: String, timestamp: Long)

case class MarketingViewCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {
    // create execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism( 1 )
    env.setStreamTimeCharacteristic( TimeCharacteristic.EventTime )

    val dataStream = env.addSource(new SimulatedEvenSource())
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "UNINSTALL")
      .map( data => {
        ((data.channel, data.behavior), 1L)
      })
      .keyBy((_._1))
      .timeWindow(Time.hours(1), Time.seconds(10))
      .process(new MarketingCountByChannel())

    dataStream.print( "market channel count" )
    env.execute( "market channel job" )
  }

  class SimulatedEvenSource extends RichSourceFunction[MartketingUserBehavior] {
    var running = true
    val behaviorTyes: Seq[String] = Seq("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL")
    val channelSets: Seq[String] = Seq("wechat", "weibo", "appstore", "huaweistore")
    val rand: Random = new Random()

    override def run(sourceContext: SourceFunction.SourceContext[MartketingUserBehavior]): Unit = {
      val maxElements = Long.MaxValue
      var count = 0L
      while (running && count < maxElements) {
        val id = UUID.randomUUID().toString
        val behavior = behaviorTyes(rand.nextInt(behaviorTyes.size))
        val channel = channelSets(rand.nextInt(channelSets.size))
        val ts = System.currentTimeMillis()
        sourceContext.collect(MartketingUserBehavior(id, behavior, channel, ts))
        count += 1
        TimeUnit.MILLISECONDS.sleep(10)
      }
    }

    override def cancel(): Unit = running = false
  }

  class MarketingCountByChannel extends ProcessWindowFunction[((String, String), Long), MarketingViewCount, (String, String), TimeWindow] {
    override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[MarketingViewCount]): Unit = {
      val startTs = new Timestamp(context.window.getStart).toString
      val endTs = new Timestamp(context.window.getEnd).toString
      val channel = key._1
      val behavior = key._2
      out.collect(MarketingViewCount(startTs, endTs, channel, behavior, elements.size))
    }
  }

}
