package com.qiao.diySort

class SecondarySortKey(val word:String , val count:Int) extends Ordered[SecondarySortKey] with Serializable {
  override def compare(that: SecondarySortKey): Int = {
    if (this.word.compareTo(that.word) != 0){
      this.word.compareTo(that.word)
    } else {
      this.count - that.count
    }
  }
}



class Tese extends Ordering[Tese]{
  override def compare(x: Tese, y: Tese): Int = ???
}