import scala.collection.mutable

class HeapItem[T](var item: T, var count: Long, var error: Long) {
  var indexInHeap: Int = -1

  override def toString: String = s"[item: $item, count: $count, error: $error]"
}

object SpaceSaving {

  def apply[T](): SpaceSaver[T] = SSEmpty()

  //apply function: every object can be treated as a function, provided it has the apply method.
  def apply[T](capacity: Int, item: T, frequency: Long): SpaceSaver[T] = SSOne(capacity, item, frequency)

}


sealed abstract class SpaceSaver[T] {

  def ++(other: SpaceSaver[T]): SpaceSaver[T]

  def topK(k: Int): Seq[(Long, Long, T)]
}

case class SSEmpty[T]() extends SpaceSaver[T] {
  def min: Long = 0L

  def capacity = 0

  def ++(other: SpaceSaver[T]): SpaceSaver[T] = other match {
    case other: SSOne[T] => other
    case other: SSMany[T] => other
    case _: SSEmpty[T] => this
  }

  override def topK(k: Int): Seq[(Long, Long, T)] = Seq.empty
}

case class SSOne[T](capacity: Int, item: T, fre: Long) extends SpaceSaver[T] {

  def ++(other: SpaceSaver[T]): SpaceSaver[T] = other match {
    case other: SSOne[_] => new SSMany[T](capacity) ++ this ++ other
    case other: SSMany[_] => other ++ this
    case _: SSEmpty[_] => this
  }

  override def topK(k: Int): Seq[(Long, Long, T)] = Seq((fre, 0, item))
}

case class SSMany[T](capacity: Int) extends SpaceSaver[T] {
  val heapSS = new HeapSS[T](capacity)

  override def ++(other: SpaceSaver[T]): SpaceSaver[T] = other match {
    case other: SSOne[T] =>
      heapSS.add(other.item, other.fre)
      this
    case other: SSMany[T] =>
      this.merge(other)
      this
    case _: SSEmpty[T] => this
  }

  private def merge(other: SSMany[T]): Unit = {
    heapSS.merge(other.heapSS)
  }

  override def topK(k: Int): Seq[(Long, Long, T)] = {
    heapSS.heap.array.
      map(item => (item.count, item.error, item.item))
      .sortBy(x => (-x._1, x._2))
      .take(k)
  }
}

class Heap[T](capacity: Int) {
  var array: Array[HeapItem[T]] = new Array[HeapItem[T]](capacity)
  var size: Int = 0

  def min(): Long = {
    if (size < capacity) {
      0L
    } else {
      array(0).count
    }
  }

  def add(heapItem: HeapItem[T]): Unit = {
    require(size < capacity)
    array(size) = heapItem
    heapItem.indexInHeap = size
    size += 1
    adjustDownToUp(size - 1)
  }

  private def swap(x: Int, y: Int): Unit = {
    require(x < size)
    require(y < size)
    val tmp = array(x)
    array(x) = array(y)
    array(y) = tmp
    array(x).indexInHeap = x
    array(y).indexInHeap = y
  }

  def adjustUpToDown(index: Int): Unit = {
    require(index < size)

    var current = index
    var child = 2 * current + 1
    if (child >= size) {
      return
    }

    while (true) {
      if (child + 1 < size && array(child + 1).count < array(child).count) {
        child += 1
      }
      if (array(child).count < array(current).count) {
        swap(child, current)
      } else {
        return
      }
      current = child
      child = 2 * current + 1
      if (child >= size) {
        return
      }
    }
  }

  def adjustDownToUp(index: Int): Unit = {
    if (index == 0) {
      return
    }
    require(index > 0)
    var current = index
    var parent = (index - 1) / 2

    while (true) {
      if (array(parent).count > array(current).count) {
        swap(parent, current)
        if (parent == 0) {
          return
        }
        current = parent
        parent = (current - 1) / 2
      } else {
        return
      }
    }
  }
}

class HeapSS[T](capacity: Int) {
  var counters: mutable.Map[T, HeapItem[T]] = mutable.Map.empty
  var size: Int = 0
  val heap: Heap[T] = new Heap[T](capacity)

  def min(): Long = heap.min()

  def merge(other: HeapSS[T]): Unit = {
    val min1 = min()
    val min2 = other.min()

    val merged = (counters.keySet ++ other.counters.keySet)
      .toList
      .map(
        key => {
          val (count1, err1) = counters.get(key) match {
            case None => (min1, min1)
            case Some(item) => (item.count, item.error)
          }
          val (count2, err2) = other.counters.get(key) match {
            case None => (min2, min2)
            case Some(item) => (item.count, item.error)
          }
          new HeapItem[T](key, count1 + count2, err1 + err2)
        }
      )
      .sortBy(x => (-x.count, x.error))
    //      .take(capacity)

    counters = mutable.Map.empty
    var index = 0
    while (index < capacity) {
      val item = merged(index)
      val indexInHeap = capacity - 1 - index
      item.indexInHeap = indexInHeap

      counters.put(item.item, item)
      heap.array(indexInHeap) = item

      index += 1
    }
  }

  def add(item: T, frequency: Long): Unit = {
    counters.get(item) match {
      case None => introduce(item, frequency);
      case Some(heapItem) => bump(heapItem, frequency)
    }
  }

  private def bump(item: HeapItem[T], frequency: Long): Unit = {
    item.count += frequency
    heap.adjustUpToDown(item.indexInHeap)
  }

  private def introduce(item: T, count: Long): Unit = {
    if (size < capacity) {
      val heapItem = new HeapItem[T](item, count, 0)
      heap.add(heapItem)
      size += 1
      counters.put(item, heapItem)
    } else {
      val minItem = heap.array(0)
      counters -= minItem.item
      minItem.error = minItem.count
      minItem.count += count
      minItem.item = item
      counters.put(item, minItem)
      heap.adjustUpToDown(0)
    }
  }
}


