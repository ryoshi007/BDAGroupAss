import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


public class FoodStockChanges {
	
		// Select the intended columns, start counting from 0
		public static final int COUNTRY_COL = 96;
		public static final int YEAR_COL = 97;
		public static final int WHEATSTOCKS_COL = 3;
		public static final int MAIZESTOCKS_COL = 27;
		public static final int BARLEYSTOCKS_COL = 51;
		public static final int SUNFLOWEROILSTOCKS_COL = 75;
	
	// Specify the countries in the WHERE clause (optional)
	public static final Set<String> SPECIFIC_COUNTRIES = new HashSet<>(
			Arrays.asList("China", "United States", "India", "Russia", "Ukraine"));
	
	// Mapper
	/**
	 * StockMapper class is static and extends Mapper abstract class and having 
	 * 2 Hadoop generic types and 2 custom types -> LongWritable (for byte offset), 
	 * Text (rows), CountryYearCompositeKey (Intermediate key after mapped), 
	 * StockWritable (Intermediate values after mapped).
	 */
	public static class StockMapper extends Mapper<LongWritable, Text, CountryYearCompositeKey, StockWritable> {
		
		/**
		 * @method map
		 * This method takes the input as Text data type.
		 * It will take country, year and stock values from specified columns.
		 * Country and year will be stored in CountryYearCompositeKey class.
		 * Stock values are stored in StockWritable class.
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			// Split each row (a record) by comma
			String[] columns = value.toString().split(",");
			
			// Check if the obtained country are in the SPECIFIC_COUNTRIES 
			String country = columns[COUNTRY_COL];
			if (!SPECIFIC_COUNTRIES.contains(country.trim())) {
				return;
			}
			
			// Retrieve relevant data from the row
			int year = Integer.parseInt(columns[YEAR_COL].trim());
			int wheatStocks = Integer.parseInt(columns[WHEATSTOCKS_COL].trim());
			int maizeStocks = Integer.parseInt(columns[MAIZESTOCKS_COL].trim());
			int barleyStocks = Integer.parseInt(columns[BARLEYSTOCKS_COL].trim());
			int sunflowerOilStocks = Integer.parseInt(columns[SUNFLOWEROILSTOCKS_COL].trim());
			
			// Create CountryYearCompositeKey and StockWritable classes
			CountryYearCompositeKey compositeKey = new CountryYearCompositeKey(country, year);
			StockWritable stockWritable = new StockWritable(wheatStocks, maizeStocks, barleyStocks, sunflowerOilStocks);
			
			// The key and value pair are stored
			context.write(compositeKey, stockWritable);
		}
	}
	
	// Partitioner
	/**
	 * CountryPartitioner class is static and extends Partitioner abstract class with
	 * CountryYearCompositeKey and StockWritable as key value pair.
	 */
	public class CountryPartitioner extends Partitioner<CountryYearCompositeKey, StockWritable> {
		
		/**
		 * @method getPartition
		 * The keys are partitioned based on the country part of the key.
		 * This ensures that all data for the same country goes to the same reducer.
		 * It calculates partition number based on hash code of the country, and then masked with
		 * Integer.MAX_VALUE to ensure a non-negative value.
		 * Modulus is used to ensure that partition number is within the range of available reduce tasks.
		 * It is used in post-mapping and pre-reducing phase.
		 */
		@Override
		public int getPartition(CountryYearCompositeKey key, StockWritable value, int numReduceTasks) {
			return (key.getCountry().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		}
	}
	
	// Comparator
	/**
	 * CountryGroupingComparator is static and extends WritableComparator abstract class.
	 * It is used for grouping values into a list for each key.
	 */
	public static class CountryGroupingComparator extends WritableComparator {
		
		// Create instances of CountryGroupingComparator automatically
		protected CountryGroupingComparator() {
			super(CountryYearCompositeKey.class, true);
		}
		
		/**
		 * @method compare
		 * Overridden the compare method so that it only compares country part of the keys.
		 * This ensures all records with the same country (regardless of year) are processed together
		 * in the reduce phase.
		 */
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			CountryYearCompositeKey key1 = (CountryYearCompositeKey) w1;
			CountryYearCompositeKey key2 = (CountryYearCompositeKey) w2;
			
			return key1.getCountry().compareTo(key2.getCountry());
		}
	} 
	
	// Reducer
	/**
	 * StockChangeReducer class is static and extends Reducer abstract class and having 
	 * 4 custom types -> CountryYearCompositeKey (Intermediate key), StockWritable (Intermediate value),
	 * CountryYearCompositeKey (Output key) and StockWritable (Output value).
	 *
	 */
	public static class StockChangeReducer extends Reducer<CountryYearCompositeKey, StockWritable, CountryYearCompositeKey, StockWritable> {
		
		/**
		 * @method reduce
		 * This method takes CountryYearCompositeKey as key and a list of StockWritable as values.
		 * It will calculate the changes of stocks in StockWritable by calculating the differences between
		 * current year and previous year.
		 */
		public void reduce(CountryYearCompositeKey key, Iterable<StockWritable> values, Context context) throws IOException, InterruptedException {
			// Create prevYearStocks variable with null value
			StockWritable prevYearStocks = null;
			
			// For stock values in every year (a StockWritable)
			for (StockWritable currentYearStocks : values) {
				// If there is no previous value in previous year, the output value will be 0
				if (prevYearStocks == null) {
					context.write(key, new StockWritable(0, 0, 0, 0));
				} else {
					// Calculate changes in stock values between current year and previous year
	                int wheatStockChange = currentYearStocks.getWheatStock() - prevYearStocks.getWheatStock();
	                int maizeStockChange = currentYearStocks.getMaizeStock() - prevYearStocks.getMaizeStock();
	                int barleyStockChange = currentYearStocks.getBarleyStock() - prevYearStocks.getBarleyStock();
	                int sunflowerOilStockChange = currentYearStocks.getSunflowerOilStock() - prevYearStocks.getSunflowerOilStock();
	                
	                // Create a new StockWritable to store the differences in stock values
	                StockWritable stockChange = new StockWritable(wheatStockChange, maizeStockChange, barleyStockChange, sunflowerOilStockChange);
	                
	                // Store the output value as pair of CountryYearCompositeKey and stockChange
	                context.write(key, stockChange);
				}
								
				// Set the prevYearStocks to the current year's values for next year comparison
				prevYearStocks = new StockWritable(currentYearStocks.getWheatStock(), currentYearStocks.getMaizeStock(), currentYearStocks.getBarleyStock(), 
						currentYearStocks.getSunflowerOilStock());
			}
		}
	}
	
	/**
	 * @method main
	 * This method is used for setting all the configuration properties.
	 * It acts as a driver for MapReduce code.
	 */
	public static void main(String[] args) throws Exception {
		
		// Read default configuration of cluster from xml files
	    Configuration conf = new Configuration();
	    
	    // Initialize job with default configuration
	    Job job = Job.getInstance(conf, "Food Stock Changes ");
	    
	    // Assign driver class name
	    job.setJarByClass(FoodStockChanges.class);
	    
	    // Define class names for mapper, reducer, partitioner and comparators
	    job.setMapperClass(StockMapper.class);
	    job.setReducerClass(StockChangeReducer.class);
	    job.setPartitionerClass(CountryPartitioner.class);
	    job.setGroupingComparatorClass(CountryGroupingComparator.class);
	    
	    // Sort partitions when merging them. It ensures each reducer has a fully
	    // sorted list of all (key, value) for all keys assigned to them
	    job.setSortComparatorClass(CountryYearCompositeKeyComparator.class);
	    
	    // Define output format class 
	    job.setOutputKeyClass(CountryYearCompositeKey.class);
	    job.setOutputValueClass(StockWritable.class);
	    
	    // Configure input and output path from file system into the job
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    // Exit the job when it's finished
	    System.exit(job.waitForCompletion(true) ? 0 : 1);		
	}
}
