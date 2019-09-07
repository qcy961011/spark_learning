package test

/**
  * 闭包Demo
  */
object ForTest {

  def mulBy(x:Double) = (y:Double) => {
    (x + 1) * y
  }

  def main(args: Array[String]): Unit = {
    val tail = mulBy(5)
    val heap = mulBy(6)
    println(tail(10))
    println(heap(50))
  }
}
