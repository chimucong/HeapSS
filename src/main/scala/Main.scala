import java.io.File

import scala.io.Source

object Main {
  def main(args: Array[String]): Unit = {
    val startTime: Long = System.currentTimeMillis
    val dir = new File("C:\\Users\\test\\Desktop\\input\\test")
    //val dir = new File("D:\\\\tmp\\\\input")

    val file = dir.listFiles() // List("D:\\tmp\\input\\a.txt","D:\\tmp\\input\\b.txt")
    //    val input = file.map(x => Source.fromFile(x).getLines()).reduce(_ ++ _)
    //          val input =Source.fromFile("D:\\Hadoop\\WorkSpace\\zipf\\test\\1000").getLines()
    val inputs = file.map(x => Source.fromFile(x).getLines())

    //val capacity = 1382
    //val capacity = 5526
    //val capacity = 12434
    //val capacity = 34539

    val topk = 150
    val alpha = 2.toDouble

    //val capacity = (math.pow((topk/alpha),(1/alpha)) * topk).toInt
    //    val capacity = (topk * topk * math.log(224023)).toInt
    //    val capacity = (topk * topk * math.log(1000000)).toInt
    val capacity = topk
    //    val sg = new SpaceSaverSemigroup[String]

    val result = inputs.foldLeft(SpaceSaving[String]()) { (acc1, input) =>
      input.foldLeft(
        acc1
      ) { (acc, itr) =>
        acc ++ SpaceSaving(capacity, itr, 1L)
      }
    }.topK(topk)
    val endTime: Long = System.currentTimeMillis

    System.out.println("程序运行时间： " + (endTime - startTime) + "ms")
    result.foreach(println)
    System.out.println("capacity： " + capacity)
  }

}