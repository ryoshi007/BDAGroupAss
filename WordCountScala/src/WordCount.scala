import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import java.lang.management.ManagementFactory
import scala.collection.JavaConverters._

object WordCount {

  def main(args: Array[String]): Unit = {
    // Set the master URL directly in SparkConf
    val conf = new SparkConf().setAppName("Word Count").setMaster("local[*]")
    
    // Create a SparkContext using the configured SparkConf
    val sc = new SparkContext(conf)
    
    val startTime = System.nanoTime()
    
    // Load dataset into an RDD
    val lines: RDD[String] = sc.textFile("hdfs://quickstart.cloudera:8020/russia-ukraine-food/ukraine-russia-food.csv")
    
    // Count number of records in RDD
    val numberOfRecords = lines.count()

    // Read input from HDFS
    val inputRDD: RDD[String] = sc.textFile(args(0))

    // Skip the header row
    val dataRDD: RDD[String] = inputRDD.zipWithIndex().filter { case (_, index) => index > 0 }.keys

    // Tokenize and count words
    val wordCountRDD: RDD[(String, Int)] = dataRDD
      .flatMap(line => line.split(","))
      .map(word => (word.trim, 1))
      .reduceByKey(_ + _)

    // Save the result to HDFS
    wordCountRDD.saveAsTextFile("hdfs://quickstart.cloudera:8020/group-output-spark")
    
    // Metrics Calculation
    val endTime = System.nanoTime()
    val duration = (endTime - startTime) / 1e9d // Convert nanoseconds to seconds
    println(s"Execution Time: $duration seconds")
    
    println(s"Number of records: $numberOfRecords")
    val throughput = numberOfRecords / duration // records per second
    println(s"Throughput: $throughput records per second")
    
    val runtime = Runtime.getRuntime
    val usedMemory = (runtime.totalMemory - runtime.freeMemory) / (1024.0 * 1024.0) // in MB
    println(s"Used Memory: $usedMemory MB")
    
    val memoryMXBean = ManagementFactory.getMemoryMXBean
    val heapMemoryUsage = memoryMXBean.getHeapMemoryUsage.getUsed / (1024.0 * 1024.0) // Convert to MB
    val nonHeapMemoryUsage = memoryMXBean.getNonHeapMemoryUsage.getUsed / (1024.0 * 1024.0) // Convert to MB
    println(s"Heap Memory Used: $heapMemoryUsage MB")
    println(s"Non-Heap Memory Used: $nonHeapMemoryUsage MB")
    
    // Prevent Spark session from closing unless 'Enter' is pressed.
    // By doing so, you can check additional Spark session information on 'http://10.0.2.15:4040/jobs/'
    // 10.0.2.15 is the IP address of the manager node, you can check it by opening the Web Browser
    println("Press ENTER to exit the program")
    scala.io.StdIn.readLine()
    
    // Stop the Spark session
    sc.stop()

  }
}

    