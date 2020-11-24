package org.example.networkflow.analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object NetworkFlow {
  def main(args: Array[String]): Unit = {
    // create execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism( 1 )
    env.setStreamTimeCharacteristic( TimeCharacteristic.EventTime )

    val dataStream = env.readTextFile( "/Users/hliu/hao/flinkexample/NetworkFlowAnalysis/src/main/resources/apache_logs" )
      .map( data => {
        val dataArray = data.split( " " )
        ApacheLogEvent( dataArray( 0 ).trim, dataArray( 1 ).trim, dataArray( 2 ).trim.toLong, dataArray( 3 ).trim, dataArray( 4 ).trim )
      } )
  }
}
