package lab1

import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.SnappyCodec
import org.apache.hadoop.io.compress.snappy.SnappyCompressor
import org.apache.hadoop.io.{IntWritable, ObjectWritable, Text}
import org.apache.hadoop.mapreduce.lib.output.{SequenceFileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.log4j.Logger

/**
 * Usage: ./program input1 [input2 ... inputN] output
 */
object MapReduceApplication extends App with Logging {
  if (args.length < 2)
    throw new RuntimeException("You should specify input and output folders!")
  val outputFolder :: inputFolders = args.map(new Path(_)).toList.reverse

  val conf = new Configuration

  val job = Job.getInstance(conf, "log severity aggregator")
  job.setJarByClass(this.getClass)
  job.setMapperClass(classOf[HW1Mapper])
  job.setReducerClass(classOf[HW1Reducer])
  job.setMapOutputKeyClass(classOf[Text])
  job.setMapOutputValueClass(classOf[IntWritable])
  job.setOutputKeyClass(classOf[Text])
  job.setOutputValueClass(classOf[Text])
  job.setOutputFormatClass(classOf[SequenceFileOutputFormat[_, _]])

  inputFolders.foreach { folder => FileInputFormat.addInputPath(job, folder) }
  FileOutputFormat.setOutputPath(job, outputFolder)
  FileOutputFormat.setCompressOutput(job, true)
  SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK)
  FileOutputFormat.setOutputCompressorClass(job, classOf[SnappyCodec])
  conf.set("mapreduce.output.fileoutputformat.compress", "true")
  conf.set("mapreduce.output.fileoutputformat.compress.type", "BLOCK")
  conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec")


  log.info("=====================JOB STARTED=====================")
  job.waitForCompletion(true)
  log.info("=====================JOB ENDED=====================")
  // проверяем статистику по счётчикам
  val counter = job.getCounters.findCounter(Counter.Malformed, "")
  log.info("=====================COUNTERS " + counter.getName + ": " + counter.getValue + "=====================")


  log.info("Hello world!")
}