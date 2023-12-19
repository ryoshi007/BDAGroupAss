import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import java.lang.management.ManagementFactory
import scala.collection.JavaConverters._

object FoodStockScala {

  // Define custom class for key
  case class CountryYearCompositeKey(country: String, year: Int){
    
    // Override toString method
    override def toString: String = s"$country\t$year"
  }
  
  // Define custom class for value
  case class StockWritable(wheatStocks: Int, maizeStocks: Int, barleyStocks: Int, sunflowerOilStocks: Int) {
    
    // Override toString Method
    override def toString: String = s"$wheatStocks\t$maizeStocks\t$barleyStocks\t$sunflowerOilStocks"
  }
  
  def main(args: Array[String]): Unit = {
    
    // Start measuring time
    val startTime = System.nanoTime()

    // Configure Spark with application name and set master as local machine
    val sparkConf = new SparkConf().setAppName("Spark Food Stock Changes")
    sparkConf.setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    
    // Load dataset into an RDD
    val lines: RDD[String] = sc.textFile("hdfs://quickstart.cloudera:8020/russia-ukraine-food/ukraine-russia-food.csv")
    
    // Count number of records in RDD
    val numberOfRecords = lines.count()
    
    // Column indices for CSV files
    val COUNTRY_COL = 96
    val YEAR_COL = 97
    val WHEATSTOCKS_COL = 3
    val MAIZESTOCKS_COL = 27
    val BARLEYSTOCKS_COL = 51
    val SUNFLOWEROILSTOCKS_COL = 75
    
    // To select specific countries (if has WHERE clause)
    val specificCountries = Set("China", "United States", "India", "Russia", "Ukraine")
    
    // Process each line to extract stock information
    // flatMap is used to handle transformation and filtering at same time by excluding None
    // map will transform every element of RDD to one element in output RDD, which will have None value
    val stocks = lines.flatMap {line =>
      val columns = line.split(",")
      val country = columns(COUNTRY_COL)
      
      if (specificCountries.contains(country.trim)) {
        val year = columns(YEAR_COL).trim.toInt
        val wheatStocks = columns(WHEATSTOCKS_COL).trim.toInt
        val maizeStocks = columns(MAIZESTOCKS_COL).trim.toInt
        val barleyStocks = columns(BARLEYSTOCKS_COL).trim.toInt
        val sunflowerOilStocks = columns(SUNFLOWEROILSTOCKS_COL).trim.toInt
        
        // If the extracted values can be made into a tuple of (key, value) pair, it will return it. Else, None value is returned.
        // A tuple must be wrapped in ()
        Some((CountryYearCompositeKey(country, year), StockWritable(wheatStocks, maizeStocks, barleyStocks, sunflowerOilStocks)))
      } else {
        None
      }
    }
    
    // Group records by country for calculation later by creating a new RDD. It is same like Partitioner in MapReduce.
    // Example: RDD(United States, List of (CountryYearCompositeKey, StockWritable))
    // groupBy is used for conditional filtering, especially in case where you have key and value with multiple variables
    // groupByKey is used if you have (key, value) pair
    // To access which variable to use, _1 refer to key, _2 refer to value, then use .variable_name to group or sort
    val groupedStocks: RDD[(String, Iterable[(CountryYearCompositeKey, StockWritable)])] = stocks.groupBy(_._1.country)
    
    // Transform groupedStocks RDD into a new RDD containing changes in stock values
    // case (k, v) is used to destructure each tuple and access its components
    // The (country, groupedRecords) here refers to (String, Iterable[(CompositeKey, StockWritable)]) above
    val stockChanges: RDD[(CountryYearCompositeKey, StockWritable)] = groupedStocks.flatMap { 
        case (country, groupedRecords) =>
      
          // Sort records for each country by year
          val sortedRecords = groupedRecords.toList.sortBy(_._1.year)

          // Initialize a variable to keep track of previous year's stocks
          var prevYearStocks = StockWritable(0, 0, 0, 0)
        
          // Map over the sorted records to calculate changes
          val changes = sortedRecords.zipWithIndex.map {
              case ((currentKey, currentYearStocks), index) =>
                
                  // If it's the first record, set change to zero
                  if (index == 0) {
                    
                    prevYearStocks = currentYearStocks
                    
                    // Return a tuple of current key and 0s for StockWritable
                    (currentKey, StockWritable(0, 0, 0, 0))
                    
                  } else {

                    // Calculate differences in stock values between current year and previous year
                    val wheatStockChange = currentYearStocks.wheatStocks - prevYearStocks.wheatStocks
                    val maizeStockChange = currentYearStocks.maizeStocks - prevYearStocks.maizeStocks
                    val barleyStockChange = currentYearStocks.barleyStocks - prevYearStocks.barleyStocks
                    val sunflowerOilStockChange = currentYearStocks.sunflowerOilStocks - prevYearStocks.sunflowerOilStocks
                    
                    // Update prevYearStocks to current year's values
                    prevYearStocks = currentYearStocks
                    
                    // Return a tuple of key and calculated stock change
                    (currentKey, StockWritable(wheatStockChange, maizeStockChange, barleyStockChange, sunflowerOilStockChange))
                  }
          }
          // Return the list of tuples after the operation
          changes
        }
    
    // Sort RDD by country and year for consistent output order
    val sortedStockChanges = stockChanges.sortBy {
        case (key, _) => (key.country, key.year)
    }
    
    // Map over the RDD by converting data into string representation
    val stringRDD = sortedStockChanges.map {
        case (key, value) => key.toString + "\t" + value.toString
    }
    
    // Save the result in hdfs folder. You may comment this line if you want to check output first in 
    // console and uncomment the code below for displaying result
    stringRDD.saveAsTextFile("hdfs://quickstart.cloudera:8020/group-output-spark")
    
//    // This code is used for displaying result on console to verify it.
//    stringRDD.collect().foreach(println)
    
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
