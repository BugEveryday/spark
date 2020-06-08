package com.testSpark.Value

object V13_pipe {
//这个是在客户端运行的
  /*
  （1）编写一个脚本，并增加执行权限
[atguigu@hadoop102 spark]$ vim pipe.sh
#!/bin/sh
echo "Start"
while read LINE; do
   echo ">>>"${LINE}
done

[atguigu@hadoop102 spark]$ chmod 777 pipe.sh
（2）创建一个只有一个分区的RDD
scala> val rdd = sc.makeRDD (List("hi","Hello","how","are","you"), 1)
（3）将脚本作用该RDD并打印
scala> rdd.pipe("/opt/module/spark/pipe.sh").collect()
res18: Array[String] = Array(Start, >>>hi, >>>Hello, >>>how, >>>are, >>>you)
（4）创建一个有两个分区的RDD
scala> val rdd = sc.makeRDD(List("hi","Hello","how","are","you"), 2)
（5）将脚本作用该RDD并打印
scala> rdd.pipe("/opt/module/spark/pipe.sh").collect()
res19: Array[String] = Array(Start, >>>hi, >>>Hello, Start, >>>how, >>>are, >>>you)

   */
}
