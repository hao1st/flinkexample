package org.example.networkflow.analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

// define input data sample class
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

// define window aggregate result sample class
case class UvCount(windowEnd: Long, count: Long)

object UvWithBloom {
  def main(args: Array[String]): Unit = {
    // create execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism( 1 )
    env.setStreamTimeCharacteristic( TimeCharacteristic.EventTime )

    // read data
    val dataStream = env.readTextFile( "/Users/hliu/hao/flinkexample/HotItemsAnalysis/src/main/resources/UserBehavior.csv" )
      .map( data => {
        val dataArray = data.split( "," )
        UserBehavior( dataArray( 0 ).trim.toLong, dataArray( 1 ).trim.toLong, dataArray( 2 ).trim.toInt, dataArray( 3 ).trim, dataArray( 4 ).trim.toLong )
      } )
      .assignAscendingTimestamps( _.timestamp * 1000L )
      .filter( _.behavior == "pv" )
//            .map(data => ("pv", 1))
//            .keyBy(_._1)
//            .timeWindow(Time.hours(10))
//            .sum(1)

//      .timeWindowAll( Time.seconds( 2) )
//      .apply( new UvCountByWindow() )

      .map( data => ("dummyKey", data.userId, data.timestamp) )
      .keyBy( _._1 )
      .timeWindow( Time.milliseconds( 1000) )
      //.trigger( new MyTrigger() )
      .process( new UvCountWithBloom() )

    dataStream.print( "uv count" )
    env.execute( "uv job" )
  }

  class UvCountByWindow extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {
    override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
      var idSet = Set[Long]()
      for (entry <- input) {
        idSet += entry.userId
      }

      out.collect( UvCount(window.getEnd, idSet.size ) )
    }
  }

  class MyTrigger extends Trigger[(String, Long, Long), TimeWindow] {
    override def onElement(t: (String, Long, Long), l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.FIRE_AND_PURGE
    }

    override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {}
  }

  class Bloom(size: Long) extends Serializable {
    //bitmap size
    private val cap = if (size > 0) size else 1 << 27

    def hash(value: String, seed: Int): Long = {
      var result = 0L
      for (i <- 0 until value.length) {
        result = result * seed + value.charAt( i )
      }
      result & (cap - 1)
    }
  }

  class UvCountWithBloom extends ProcessWindowFunction[(String, Long, Long), UvCount, String, TimeWindow] {
    //redis connection
    lazy val jedis = new Jedis( "localhost", 6379 )
    lazy val bloom = new Bloom( 1 << 29 ) //64M bitmap -> 512 M keys
    override def process(key: String, context: Context, elements: Iterable[(String, Long, Long)], out: Collector[UvCount]): Unit = {
      val storeKey = context.window.getEnd.toString //elements.last._3.toString
      var count = 0L
      if (jedis.hget( "count", storeKey ) != null) {
        count = jedis.hget( "count", storeKey ).toLong
      }
      for (e <- elements) {
        val userId = e._2.toString
        //val userId = elements.last._2.toString
        val offset = bloom.hash( userId, 61 );

        val isExist = jedis.getbit( storeKey, offset )
        if (!isExist) {
          jedis.setbit( storeKey, offset, true)
          count += 1
          jedis.hset( "count", storeKey, count.toString )
        }
      }

      out.collect( UvCount( storeKey.toLong, count ) )
    }
  }

}
