package lab1

import org.apache.hadoop.io.{IntWritable, LongWritable, MapWritable, ObjectWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

import java.io.IOException
import java.sql.Timestamp
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime, LocalTime}

class HW1Mapper extends Mapper[LongWritable, Text, ObjectWritable, IntWritable] {
  private val UnknownSeverityLevel = -1

  @throws[IOException]
  @throws[InterruptedException]
  override protected def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, ObjectWritable, IntWritable]#Context): Unit = {
    val (time, severityLevel) =
      value.toString.split(" ").take(3).toSeq match {
        case rawDate :: rawTime :: rawSeverity :: Nil =>
          val time = LocalDate.parse(rawDate) atTime LocalTime.parse(rawTime) truncatedTo ChronoUnit.HOURS
          val severityLevel = rawSeverity match {
            case "emerg" | "panic" => 0
            case "alert" => 1
            case "crit" => 2
            case "err" | "error" => 3
            case "warn" | "warning" => 4
            case "notice" => 5
            case "info" => 6
            case "debug" => 7
            case _ => UnknownSeverityLevel
          }

          (time, severityLevel)
      }

    if (severityLevel == UnknownSeverityLevel)
      context.getCounter(Counter.Malformed, "").increment(1)
    else
      context.write(TimeWritable(time), new IntWritable(severityLevel))
  }
}

case class TimeWritable(time: LocalDateTime) extends ObjectWritable

object Counter {
  final val Malformed: String = "Malformed"
}
