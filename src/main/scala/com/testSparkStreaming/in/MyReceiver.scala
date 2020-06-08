package com.testSparkStreaming.in

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class MyReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){
//  启动时调用的方法，作用为：读数据并将数据发送给Spark
  def receSend() = {
    try {
      val socket = new Socket(host, port)//构造函数传值

      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))
      //    读取数据
      var input: String = reader.readLine()

      while (input != null && !isStopped()) {
        //      写入spark内存
        store(input)
        //      重新读取新的数据
        input = reader.readLine()
      }

      //    如果异常
      reader.close()
      socket.close()
      restart("重启一下")
    } catch {
      case e:Exception =>restart("exception")
      case t:Throwable =>restart("throwable")
    }
  }
  override def onStart(): Unit = {
    new Thread(){
      override def run(){
        receSend()
      }
    }.start()
  }
//关闭时调用的方法
  override def onStop(): Unit = ???
}
