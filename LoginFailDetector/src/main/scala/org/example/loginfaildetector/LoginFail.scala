package org.example.loginfaildetector

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

case class LoginEvent(userId: Long, ip: String, eventType:String, eventTime: Long)

case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

object LoginFail {
  def main(args: Array[String]): Unit = {
    // create execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism( 1 )
    env.setStreamTimeCharacteristic( TimeCharacteristic.EventTime )

    val resource = getClass().getResource( "/LoginLog.csv" )
    val eventStream = env.readTextFile( resource.getFile )
      .map( data => {
        val dataArray = data.split( "," )
        LoginEvent( dataArray( 0 ).trim.toLong, dataArray( 1 ).trim, dataArray( 2 ).trim, dataArray( 3 ).trim.toLong )
      } )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(60)) {
        override def extractTimestamp(element: LoginEvent): Long =
          element.eventTime * 1000L
      })
      //.assignAscendingTimestamps( _.eventTime * 1000L )

    val warningStream = eventStream
      .keyBy( _.userId )
      .process( new LoginWarning( 2 ) )

    warningStream.print()
    env.execute("login fail detect job")
  }
}

class LoginWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning] {
  lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-state", classOf[LoginEvent]))
  override def processElement(i: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, collector: Collector[Warning]): Unit = {

  }
}
