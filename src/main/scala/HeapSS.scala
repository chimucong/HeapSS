import scala.collection.mutable

object SpaceSaving {
  def apply[T](): SpaceSaver[T] = SSEmpty()

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
  //内部使用的是HeapSS，为了与之前的程序一致，用SSMany包装了一下
  val heapSS = new HeapSS[T](capacity)

  override def ++(other: SpaceSaver[T]): SpaceSaver[T] = other match {
    case other: SSOne[T] =>
      //单个Item加入
      heapSS.add(other.item, other.fre)
      this
    case other: SSMany[T] =>
      //两个SSMany合并
      this.merge(other)
      this
    case _: SSEmpty[T] => this
  }

  private def merge(other: SSMany[T]): Unit = {
    //合并实际上是heapSS的合并
    heapSS.merge(other.heapSS)
  }

  override def topK(k: Int): Seq[(Long, Long, T)] = {
    heapSS.heap.array. //所有的Item是存放在 heapSS.heap.array 数组里
      map(item => (item.count, item.error, item.item)) //先转换成 (count,error,itme)格式
      .sortBy(x => (-x._1, x._2)) //排序，按照count降序，如果count相同，按照error升序
      .take(k) //取前k个
  }
}

// 在Heap.array 保存的对象
class HeapItem[T](var item: T, var count: Long, var error: Long) {
  //比普通的 (count,error,item)多了index一项，记录该对象在Heap.array中的位置
  var indexInHeap: Int = -1 //初始值为-1，在插入Heap.array中时会更新为实际的位置的索引

  override def toString: String = s"[item: $item, count: $count, error: $error]"
}

class Heap[T](capacity: Int) {
  // 堆排序所用到的数组
  var array: Array[HeapItem[T]] = new Array[HeapItem[T]](capacity)
  // 当前 HeapItem 的数目
  var size: Int = 0

  // 计数器的最小值
  def min(): Long = {
    if (size < capacity) {
      // 如果计数器 (HeapItem) 的数目还没有达到指定的数目，则计数器最小值为0
      0L
    } else {
      // 否则返回数组中最小的计数值
      // 按照堆排序（这里用的是最小堆）的规则，最小值为数组中第0个计数器的值
      array(0).count
    }
  }

  // 向堆中添加计数器
  def add(heapItem: HeapItem[T]): Unit = {
    require(size < capacity)
    // 首先将计数器添加至数据末尾
    array(size) = heapItem
    // 更新计数器在数组中的index
    heapItem.indexInHeap = size
    size += 1
    // 由于往数组的末尾添加了计数器，需要调整堆，使其重新平衡
    adjustDownToUp(size - 1)
  }

  // 交换数组中两个计数器的位置
  private def swap(x: Int, y: Int): Unit = {
    require(x < size)
    require(y < size)
    val tmp = array(x)
    array(x) = array(y)
    array(y) = tmp
    array(x).indexInHeap = x
    array(y).indexInHeap = y
  }

  // 调整堆，使计数器下移
  def adjustUpToDown(index: Int): Unit = {
    require(index < size)

    var current = index
    // 计算左子节点index
    var child = 2 * current + 1
    // 如果左子结点不存在，则不用调整
    if (child >= size) {
      return
    }

    while (true) {
      // 如果右子节点存在，且比左子结点小，那么选择右子节点作为预交换的子节点
      if (child + 1 < size && array(child + 1).count < array(child).count) {
        child += 1
      }
      if (array(child).count < array(current).count) {
        // 如果预交换的子节点确实比当前节点小，则进行交换
        swap(child, current)
      } else {
        //否则，堆已经平衡
        return
      }

      // 将交换后的子节点位置作为当前节点，重复上述步骤
      current = child
      child = 2 * current + 1
      if (child >= size) {
        return
      }
    }
  }

  // 调整堆，使计数器上移
  def adjustDownToUp(index: Int): Unit = {
    if (index == 0) {
      // 要调整的为根节点，则无需调整
      return
    }
    require(index > 0)
    var current = index
    // 计算父节点的index
    var parent = (index - 1) / 2

    while (true) {
      if (array(parent).count > array(current).count) {
        // 如果父节点比当前节点大，则交换
        swap(parent, current)
        if (parent == 0) {
          // 如果父节点是根节点了，则停止调整
          return
        }
        // 设置父节点为当前节点，重复上述步骤
        current = parent
        parent = (current - 1) / 2
      } else {
        // 堆已经平衡，退出
        return
      }
    }
  }
}

class HeapSS[T](capacity: Int) {
  // hashmap 根据 Item 快速找到计数器 （HeapItem）
  var counters: mutable.Map[T, HeapItem[T]] = mutable.Map.empty
  // 当前计数器数目
  var size: Int = 0
  // 堆排序使用的堆
  val heap: Heap[T] = new Heap[T](capacity)

  // 计数器的最小值
  def min(): Long = heap.min()

  //　合并其他HeapSS，步骤同之前程序类似
  def merge(other: HeapSS[T]): Unit = {
    val min1 = min()
    val min2 = other.min()

    // merged 为合并之后的计数器（HeapItem）数组，降序排列
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

    // 重新创建hashmap
    counters = mutable.Map.empty
    var index = 0
    while (index < capacity) {
      val item = merged(index)
      // HeapItem在Heap.array中为升序排列
      val indexInHeap = capacity - 1 - index
      item.indexInHeap = indexInHeap

      counters.put(item.item, item)
      heap.array(indexInHeap) = item

      index += 1
    }
  }

  // 添加Item
  def add(item: T, frequency: Long): Unit = {
    counters.get(item) match {
      // 如果为新的Item
      case None => introduce(item, frequency);
      // 如果该Item的计数器已经存在
      case Some(heapItem) => bump(heapItem, frequency)
    }
  }

  // 更新计数器的计数值
  private def bump(item: HeapItem[T], frequency: Long): Unit = {
    item.count += frequency
    // 重新调整堆，由于是将计数值增大，需要进行下移
    heap.adjustUpToDown(item.indexInHeap)
  }

  // 添加新的Item
  private def introduce(item: T, count: Long): Unit = {
    if (size < capacity) {
      // 如果当前计数器数目未达到指定的数目，则添加计数器
      val heapItem = new HeapItem[T](item, count, 0)
      heap.add(heapItem)
      size += 1
      counters.put(item, heapItem)
    } else {
      // 否则，修改计数值最小的计数器，也即 heap.array(0)
      val minItem = heap.array(0)
      // 将要替换出去的Item从hashmap中移除
      counters -= minItem.item
      // 误差为之前的计数值
      minItem.error = minItem.count
      // 当前的计数值为原先的计数值加上新Item的计数值
      minItem.count += count
      // 将计数器中的Item替换为新的Item
      minItem.item = item
      // 将新Item加入hashmap
      counters.put(item, minItem)
      // 由于替换Item后导致计数器的计数值增加，需要调整堆，将计数器下移
      heap.adjustUpToDown(0)
    }
  }
}