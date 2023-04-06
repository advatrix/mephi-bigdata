import eu.bitwalker.useragentutils.UserAgent
import lab1.{HW1Mapper, HW1Reducer}
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mrunit.mapreduce.MapDriver
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver
import org.junit.Before
import org.junit.Test

import java.io.IOException
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalTime}
import java.util
import java.util.{ArrayList, List}


class MapReduceTest {
  private var mapDriver: MapDriver[LongWritable, Text, Text, IntWritable] = _
  private var reduceDriver: ReduceDriver[Text, IntWritable, Text, Text] = _
  private var mapReduceDriver : MapReduceDriver[LongWritable, Text, Text, IntWritable, Text, Text]= _
  private val testMalformedIP = "mama mila ramu"
  private val testIP = "2023-04-05 14:55:47 [notice]\tapplication: bla bla bla"
  private val testIP2 = "2023-04-05 14:55:47 [warn]\tapplication: bla bla bla"

  private val TestTime = new Text(
    LocalDate.parse("2023-04-05")
      atTime LocalTime.parse("14:55:47")
      truncatedTo ChronoUnit.HOURS
      format HW1Mapper.DateHourFormatter
  )

  @Before def setUp(): Unit = {
    val mapper = new HW1Mapper
    val reducer = new HW1Reducer
    mapDriver = MapDriver.newMapDriver(mapper)
    reduceDriver = ReduceDriver.newReduceDriver(reducer)
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer)
  }

  @Test
  @throws[IOException]
  def testMapper(): Unit = {
    mapDriver
      .withInput(new LongWritable, new Text(testIP))
      .withOutput(TestTime, new IntWritable(5))
      .runTest()
  }

  @Test
  @throws[IOException]
  def testReducer(): Unit = {
    val values = new util.ArrayList[IntWritable]
    values.add(new IntWritable(1))
    values.add(new IntWritable(1))

    reduceDriver
      .withInput(TestTime, values)
      .withOutput(TestTime, new Text("1: 2"))
      .runTest()
  }

  @Test
  @throws[IOException]
  def testMapReduce(): Unit = {
    mapReduceDriver
      .withInput(new LongWritable, new Text(testIP))
      .withInput(new LongWritable, new Text(testIP))
      .withInput(new LongWritable, new Text(testIP2))
      .withOutput(TestTime, new Text("4: 1, 5: 2"))
      .runTest()
  }
}
