package Streaming

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by jack_chou on 2018/12/4.
  */
object WordCountDemo {

  def main(args: Array[String]) {

    val  conf = new SparkConf().setAppName("WordCountDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 创建了一个streamingContext对象
    val ssc = new StreamingContext(sc,Seconds(5))

     val textStream: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.111.100",2999)

    val resDstream  = textStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    resDstream.print() // 打印输出,是一个outputs操作，类似于action算子

    ssc.start()

    ssc.awaitTermination()
  }

}
