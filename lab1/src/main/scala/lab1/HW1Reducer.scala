package lab1

import org.apache.hadoop.io.{IntWritable, MapWritable, ObjectWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import java.io.IOException
import java.lang

class HW1Reducer extends Reducer[Text, IntWritable, Text, Text] {
  @throws[IOException]
  @throws[InterruptedException]
  override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, Text]#Context): Unit = {
    val stats = scala.collection.mutable.Map.empty[Int, Int]

    while (values.iterator.hasNext)
      stats.updateWith(values.iterator.next.get) {
        case Some(count) => Some(count + 1)
        case _ => Some(1)
      }

    val result = new Text(
      stats.toSeq.sortBy(_._1) map { case severity -> count => s"$severity: $count"} mkString ", "
    )
    context.write(key, result)
  }

}
