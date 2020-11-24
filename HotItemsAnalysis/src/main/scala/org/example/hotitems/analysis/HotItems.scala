package org.example.hotitems.analysis

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

// define input data sample class
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

// define window aggregate result sample class
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItems {
  def main(args: Array[String]): Unit = {
    // create execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism( 1 )
    env.setStreamTimeCharacteristic( TimeCharacteristic.EventTime )

    val properties = new Properties();
    properties.setProperty( "bootstrap.servers", "localhost:9092" )
    properties.setProperty( "group.id", "consumer-group" )
    properties.setProperty( "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer" )
    properties.setProperty( "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer" )
    properties.setProperty( "auto.offset.reset", "latest" )

    val resource = getClass().getClassLoader().getResource( "UserBehavior.csv" )

    // read data
    val dataStream = env.readTextFile( resource.getFile )
      //val dataStream = env.addSource(new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties))
      .map( data => {
        val dataArray = data.split( "," )
        UserBehavior( dataArray( 0 ).trim.toLong, dataArray( 1 ).trim.toLong, dataArray( 2 ).trim.toInt, dataArray( 3 ).trim, dataArray( 4 ).trim.toLong )
      } )
      .assignAscendingTimestamps( _.timestamp * 1000L )
    /*         .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.seconds(60)) {
              override def extractTimestamp(userBehavior: UserBehavior): Long =
                userBehavior.timestamp * 1000L
          })*/
    // transform
    val processedStream = dataStream
      /*      .filter(new FilterFunction[UserBehavior]() {
              override def filter(userBehavior: UserBehavior): Boolean = (userBehavior == "pv")
            })*/
      .filter( _.behavior == "pv" )
      .keyBy( _.itemId )
      .timeWindow( Time.hours( 5 ), Time.minutes( 60 ) )
      .aggregate( new CountAgg(), new WindowResult() )
      .keyBy( _.windowEnd )
      .process( new TopNHotItems( 3 ) )

    // sink
    processedStream.print

    env.execute( "Hot items job" )
  }
}

class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class AvgAgg() extends AggregateFunction[UserBehavior, (Long, Int), Double] {
  override def createAccumulator(): (Long, Int) = (0L, 0)

  override def add(in: UserBehavior, acc: (Long, Int)): (Long, Int) = (acc._1 + in.timestamp, acc._2 + 1)

  override def getResult(acc: (Long, Int)): Double = acc._1 / acc._2

  override def merge(acc: (Long, Int), acc1: (Long, Int)): (Long, Int) = (acc._1 + acc1._1, acc._2 + acc1._2)
}

class WindowResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect( ItemViewCount( key, window.getEnd, input.iterator.next ) )
  }
}

class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {
  private var itemState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext.getListState( new ListStateDescriptor[ItemViewCount]( "item-state", classOf[ItemViewCount] ) )
  }

  override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    itemState.add( i )
    context.timerService().registerEventTimeTimer( i.windowEnd + 1 );
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val allItems: ListBuffer[ItemViewCount] = new ListBuffer()
    import scala.collection.JavaConversions._
    for (item <- itemState.get()) {
      allItems += item
    }
    val sortedItems = allItems.sortBy( _.count )( Ordering.Long.reverse ).take( topSize )

    itemState.clear();
    val result: StringBuilder = new StringBuilder();
    result.append( "time: " ).append( new Timestamp( timestamp - 1 ) ).append("\n")
    for (i <- sortedItems.indices) {
      val currItem = sortedItems( i )
      result.append( "  No" ).append( i + 1 ).append( ":" )
        .append( "  id: " ).append( currItem.itemId )
        .append( "  count: " ).append( currItem.count ).append( "\n" )
    }
    result.append("============================")
    Thread.sleep(200)
    out.collect(result.toString())
  }
}
