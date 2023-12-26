import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import java.lang.management.ManagementFactory
import scala.collection.JavaConverters._

object ImportsTotalScala {
    private val COUNTRY_COL = 96
    private val WHEAT_IMPORTS_COL = 2
    private val MAIZE_IMPORTS_COL = 26
    private val BARLEY_IMPORTS_COL = 50
    private val SUNFLOWER_OIL_IMPORTS_COL = 74
    
    private val NON_COUNTRIES = Set("World", "Asia", "Africa", "Europe", "Oceania", "South America")
  
    case class ImportsWritable(wheatImports: Int, maizeImports: Int, barleyImports: Int, sunflowerOilImports: Int) {
        def getTotalImports: Int = wheatImports + maizeImports + barleyImports + sunflowerOilImports

        override def toString: String = f"$wheatImports%-8d$maizeImports%-8d$barleyImports%-8d$sunflowerOilImports%-8d${getTotalImports}%-8d"
    }
    
    def main(args: Array[String]): Unit = {
        val startTime = System.nanoTime()
      
        val conf = new SparkConf().setAppName("ImportsTotal")
        conf.setMaster("local[*]")
        val sc = new SparkContext(conf)

        val inputPath = args(0)
        val outputPath = args(1)
    
        val rawData: RDD[String] = sc.textFile(inputPath)
        val numberOfRecords = rawData.count()
        
        val data: RDD[String] = rawData.zipWithIndex().filter { case (_, index) => index > 0 }.keys
        
        val importsData: RDD[(String, ImportsWritable)] = data.flatMap(line => {
            val tokens = line.split(",")
            
            val country = tokens(COUNTRY_COL)
            if (NON_COUNTRIES.contains(country)) {
                None
            } else {                
                val wheatImports = if (tokens(WHEAT_IMPORTS_COL).isEmpty) 0 else tokens(WHEAT_IMPORTS_COL).toInt
                val maizeImports = if (tokens(MAIZE_IMPORTS_COL).isEmpty) 0 else tokens(MAIZE_IMPORTS_COL).toInt
                val barleyImports = if (tokens(BARLEY_IMPORTS_COL).isEmpty) 0 else tokens(BARLEY_IMPORTS_COL).toInt
                val sunflowerOilImports = if (tokens(SUNFLOWER_OIL_IMPORTS_COL).isEmpty) 0 else tokens(SUNFLOWER_OIL_IMPORTS_COL).toInt
        
                Some(country, ImportsWritable(wheatImports, maizeImports, barleyImports, sunflowerOilImports))
            }
        })

        val totalImports: RDD[(String, ImportsWritable)] = importsData.reduceByKey {
            case (imports1, imports2) =>
                ImportsWritable(
                  imports1.wheatImports + imports2.wheatImports,
                  imports1.maizeImports + imports2.maizeImports,
                  imports1.barleyImports + imports2.barleyImports,
                  imports1.sunflowerOilImports + imports2.sunflowerOilImports
                )
        }
        
        val sortedTotalImports: RDD[(String, ImportsWritable)] = totalImports.sortBy { case (key, _) => key }
        
        val output: RDD[String] = sortedTotalImports.map {
            case (key, value) =>
                f"${key.toString}%-35s\t${value.wheatImports}%9d\t${value.maizeImports}%9d\t${value.barleyImports}%9d\t${value.sunflowerOilImports}%9d\t${value.getTotalImports}%9d"
        }
    
        output.coalesce(1).saveAsTextFile(outputPath)
        
        // Metrics Calculation
        val endTime = System.nanoTime()
        val duration = (endTime - startTime) / 1e9d
        println(f"Execution Time: ${duration}%.3f seconds")
        
        val runtime = Runtime.getRuntime
        val usedMemory = (runtime.totalMemory - runtime.freeMemory) / (1024.0 * 1024.0)
        println(f"Memory Used: ${usedMemory}%.3f MB")
        
        val memoryMXBean = ManagementFactory.getMemoryMXBean
        val heapMemoryUsage = memoryMXBean.getHeapMemoryUsage.getUsed / (1024.0 * 1024.0)
        val nonHeapMemoryUsage = memoryMXBean.getNonHeapMemoryUsage.getUsed / (1024.0 * 1024.0)
        println(f"Heap Memory Used: ${heapMemoryUsage}%.3f MB")
        println(f"Non-Heap Memory Used: ${nonHeapMemoryUsage}%.3f MB")
        
        val throughput = numberOfRecords / duration
        println(f"Throughput: ${throughput}%.3f records per second")
    
        sc.stop()
    }
}
