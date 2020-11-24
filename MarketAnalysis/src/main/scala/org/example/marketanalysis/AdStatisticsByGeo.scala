package org.example.marketanalysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class AdClickEvent(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

case class CountByProvince( windowEnd: String, province: String, count: Long)

case class BlackListWarning(userId: Long, adId: Long, msg: String)

object AdStatisticByGeo {
  val blackListOutputTag: OutputTag[BlackListWarning] = new OutputTag[BlackListWarning]( "blacklist" )

  def main(args: Array[String]): Unit = {
    // create execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism( 1 )
    env.setStreamTimeCharacteristic( TimeCharacteristic.EventTime )

    val resource = getClass().getResource( "/AdClickLog.csv" )
    val adEventStream = env.readTextFile( resource.getFile )
      .map( data => {
        val dataArray = data.split( "," )
        AdClickEvent( dataArray( 0 ).trim.toLong, dataArray( 1 ).trim.toLong, dataArray( 2 ).trim, dataArray( 3 ).trim, dataArray( 4 ).trim.toLong )
      } )
      .assignAscendingTimestamps( _.timestamp * 1000L )

    //filter frequently click users for same ad
    val filterBlackListStream = adEventStream
      .keyBy( data => (data.userId, data.adId) )
      .process( new FilterBlackListUser( 5 ) )


    val adCountStream = filterBlackListStream
      .keyBy( _.province )
      .timeWindow( Time.minutes( 60 ), Time.seconds( 30 ) )
      .aggregate( new AdCountAgg(), new AdCountResult() )


    adCountStream.print( "ad click statistics by geo" )
    filterBlackListStream.getSideOutput( blackListOutputTag ).print( "blacklist" )

    env.execute( "ad statistics by geo job" )
  }

  class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent] {
    lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state", classOf[Long]))
    lazy val isSentBlackList: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isSent-state", classOf[Boolean]))
    lazy val resetTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resetTimer-state", classOf[Long]))

    override def processElement(i: AdClickEvent, context: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, collector: Collector[AdClickEvent]): Unit = {
      val currCount = countState.value()
      if (currCount == 0) {
        val ts = i.timestamp + 60 * 60
        //val ts = context.timerService().currentProcessingTime() + 60 * 60 * 1000
        //val ts = (context.timerService().currentProcessingTime()/(1000*60*60*24) + 1) * (1000*60*60*24)
        resetTimer.update(ts);
        context.timerService().registerProcessingTimeTimer(ts)
      }

      if (currCount >= maxCount) {
        if (!isSentBlackList.value()) {
          isSentBlackList.update(true)
          context.output(blackListOutputTag, BlackListWarning(i.userId, i.adId, "Click over " + maxCount + " times today."))
          return
        }
      }
      countState.update(currCount + 1)
      collector.collect(i)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]) {
     // super.onTimer( timestamp, ctx, out )
      if (timestamp == resetTimer.value()) {
        isSentBlackList.clear()
        countState.clear()
        resetTimer.clear()
      }
    }
    }
}

class AdCountAgg() extends AggregateFunction[AdClickEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: AdClickEvent, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class AdCountResult extends WindowFunction[Long, CountByProvince, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
    val endTs = new Timestamp( window.getEnd ).toString
    out.collect(new CountByProvince(endTs, key, input.iterator.next))
  }
}
