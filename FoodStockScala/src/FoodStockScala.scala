import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import java.lang.management.ManagementFactory
import scala.collection.JavaConverters._

object FoodStockScala {
  private val COUNTRY_COL = 96
  private val YEAR_COL = 97
  private val WHEATSTOCKS_COL = 3
  private val MAIZESTOCKS_COL = 27
  private val BARLEYSTOCKS_COL = 51
  private val SUNFLOWEROILSTOCKS_COL = 75
  
  private val specificCountries = Set("China", "United States", "India", "Russia", "Ukraine")
  
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
    
    val startTime = System.nanoTime()
    
    val sparkConf = new SparkConf().setAppName("Spark Food Stock Changes")
    sparkConf.setMaster("local[*]")
    val sc = new SparkContext(sparkConf)    
    
    val inputPath = args(0)
    val outputPath = args(1) 
    
    val lines: RDD[String] = sc.textFile(inputPath)
    
    val numberOfRecords = lines.count()
    
    val stocks = lines.flatMap(line => { 
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
    })
    
    val groupedStocks: RDD[(String, Iterable[(CountryYearCompositeKey, StockWritable)])] = stocks.groupBy(_._1.country)
    
    val stockChanges: RDD[(CountryYearCompositeKey, StockWritable)] = groupedStocks.flatMap{
      case (country, groupedRecords) => 
        
        val sortedRecords = groupedRecords.toList.sortBy(_._1.year)
        
        val initialRecord = sortedRecords.headOption.map {
          case (key, _) => (key, StockWritable(0, 0, 0, 0))
        }
        
        val changes = sortedRecords.sliding(2).collect {
          case List((prevYearKey, prevYearStocks), (currentYearKey, currentYearStocks)) =>
            val wheatStockChange = currentYearStocks.wheatStocks - prevYearStocks.wheatStocks
            val maizeStockChange = currentYearStocks.maizeStocks - prevYearStocks.maizeStocks
            val barleyStockChange = currentYearStocks.barleyStocks - prevYearStocks.barleyStocks
            val sunflowerOilStockChange = currentYearStocks.sunflowerOilStocks - prevYearStocks.sunflowerOilStocks
            (currentYearKey, StockWritable(wheatStockChange, maizeStockChange, barleyStockChange, sunflowerOilStockChange))
        }
        
        initialRecord.toList ++ changes
        
    }
    
    val sortedStockChanges = stockChanges.sortBy { case (key, _) => (key.country, key.year)}
    val stringRDD = sortedStockChanges.map {case (key, value) => key.toString + "\t" + value.toString}
    stringRDD.saveAsTextFile(outputPath)
    
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
    
    // Stop the Spark session
    sc.stop()
  }
  
}
