package com.test.day04

object Day0405 {
  def main(args: Array[String]): Unit = {
    val list=List(1,2,3,4,6,7,85,4,22,3)


    println("（1）获取集合长度"+list.length)
    println("（2）获取集合大小"+list.size)
//    （3）循环遍历
    list.foreach(println)
    println("（4）迭代器"+list.iterator)
    println("（5）生成字符串"+list.mkString(","))
    println("（6）是否包含"+list.contains(2))

    println("（1）获取集合的头"+list.head)
    println("（2）获取集合的尾（不是头的就是尾）"+list.tail)
//    （3）集合最后一个数据
//    （4）集合初始数据（不包含最后一个）
    println("（5）反转"+list.reverse)
    println("（6）取前（后）n个元素"+list.take(3)+";"+list.takeRight(3))
    println("（7）去掉前（后）n个元素"+list.drop(2)+";"+list.dropRight(2))
    println("（8）并集"+list.intersect(List(2,3,4,5,2)))
    println("（9）交集"+list.union(List(3,4,2,5,2)))
    println("（10）差集"+list.diff(List(3,2,5,32)))
    println("（11）拉链"+list.zip(List(1,2,3,4,6,7,85,4,22,3)))
//    （12）滑窗

    println("（1）求和"+list.sum)
    println("（2）求乘积"+list.reduce(_*_))
    println("（3）最大值"+list.max)
    println("（4）最小值"+list.min)
    println("（5）排序"+list.sorted)

    println("（1）过滤"+list.filter(_>5))
    println("（2）转化/映射"+list.map((_,1)))
//    println("（3）扁平化"+list.flatten)
//    println("（4）扁平化 + 映射 注：flatMap相当于先进行map操作，在进行flatten操作"+list.flatMap(_+1))
//    （5）分组
//    （6）简化（规约）
//    （7）折叠
  }

}
