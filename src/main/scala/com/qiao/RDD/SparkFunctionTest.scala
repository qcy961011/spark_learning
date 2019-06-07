package com.qiao.RDD

import com.qiao.BaseSpark


object SparkFunctionTest extends BaseSpark {
  def main(args: Array[String]): Unit = {
    sc.master
    val arr = sc.parallelize(Array(1, 2, 3, 4, 4 , 5, 6, 7) , 4)
    println(arr.getNumPartitions)
    println("=============================")

    val test = Array(1, 2, 3, 4, 4 , 5, 6, 7)
    println(arr.collect().toList)

    println("=============================")

    println(arr.count())

    println("=============================")

    println(arr.countByValue())

    println("=============================")

    println(arr.take(3).toList)

    println("=============================")

    println(arr.first())

    println("=============================")

    println(arr.top(2).toList)

    println("=============================")

    println(arr.takeOrdered(2).toList)

    println("=============================")

    println(arr.reduce((a , b) => a + b))

    println("=============================")

    println(arr.fold(100)(_ + _))

    println("=============================")

    println(arr.aggregate(100)((a,b) => a + b , (a , b) => a * b))

    println("=============================")

    println(arr.foreach(x => print(x * 100 + "\t")))

    println("=============================")

    println(test.fold(100)(_ + _))

    println("=============================")

    println(test.par.fold(100)(_ + _))

    println("=============================")

    var func = (index:Int , iter:Iterator[Int]) => {
      iter.map("[partID : " + index + " , val :" + _ + "]" )
    }
    print(arr.mapPartitionsWithIndex(func).collect().toList)

  }
}
