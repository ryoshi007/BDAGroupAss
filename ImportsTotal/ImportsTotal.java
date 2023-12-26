import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ImportsTotal {
	private static final int COUNTRY_COL = 96;
	private static final int WHEAT_IMPORTS_COL = 2;
	private static final int MAIZE_IMPORTS_COL = 26;
	private static final int BARLEY_IMPORTS_COL = 50;
	private static final int SUNFLOWER_OIL_IMPORTS_COL = 74;
	
	private static final Set<String> NON_COUNTRIES = new HashSet<>(
		Arrays.asList("World", "Asia", "Africa", "Europe", "Oceania", "South America")
	);
	
	public static enum Counter {
	    RECORD_COUNT
	}
	
	private static class ImportsWritable implements Writable {
		private int wheatImports;
		private int maizeImports;
		private int barleyImports;
		private int sunflowerOilImports;
		
		public ImportsWritable() {}
		
		public ImportsWritable(int wheatImports, int maizeImports, int barleyImports, int sunflowerOilImports) {
			this.wheatImports = wheatImports;
			this.maizeImports = maizeImports;
			this.barleyImports = barleyImports;
			this.sunflowerOilImports = sunflowerOilImports;
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			wheatImports = in.readInt();
			maizeImports = in.readInt();
			barleyImports = in.readInt();
			sunflowerOilImports = in.readInt();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(wheatImports);
			out.writeInt(maizeImports);
			out.writeInt(barleyImports);
			out.writeInt(sunflowerOilImports);
		}

		public int getWheatImports() {
			return wheatImports;
		}

		public int getMaizeImports() {
			return maizeImports;
		}

		public int getBarleyImports() {
			return barleyImports;
		}

		public int getSunflowerOilImports() {
			return sunflowerOilImports;
		}
		
		public int getTotalImports() {
			return wheatImports + maizeImports + barleyImports + sunflowerOilImports;
		}
		
		public String toString() {
			return String.format("%d\t%d\t%d\t%d\t%d",
					getWheatImports(), getMaizeImports(), getBarleyImports(), getSunflowerOilImports(), getTotalImports());
		}
	}
	
	private static class TokenizerMapper extends Mapper<LongWritable, Text, Text, ImportsWritable> {
		private Text country = new Text(); 
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.getCounter(Counter.RECORD_COUNT).increment(1);
			
			if (key.get() == 0) return;
			
			String[] tokens = value.toString().split(",");
			
			if (NON_COUNTRIES.contains(tokens[COUNTRY_COL])) return;
			
			country.set(tokens[COUNTRY_COL]);
	    	int wheatImports = tokens[WHEAT_IMPORTS_COL].isEmpty() ? 0 : Integer.parseInt(tokens[WHEAT_IMPORTS_COL]);
	    	int maizeImports = tokens[MAIZE_IMPORTS_COL].isEmpty() ? 0 : Integer.parseInt(tokens[MAIZE_IMPORTS_COL]);
	    	int barleyImports = tokens[BARLEY_IMPORTS_COL].isEmpty() ? 0 : Integer.parseInt(tokens[BARLEY_IMPORTS_COL]);
	    	int sunflowerOilImports = tokens[SUNFLOWER_OIL_IMPORTS_COL].isEmpty() ? 0 : Integer.parseInt(tokens[SUNFLOWER_OIL_IMPORTS_COL]);

	    	ImportsWritable imports = new ImportsWritable(wheatImports, maizeImports, barleyImports, sunflowerOilImports);
	    	context.write(country, imports);
	    }
	}
	
	private static class ImportSumReducer extends Reducer<Text, ImportsWritable, Text, ImportsWritable> {
		public void reduce(Text key, Iterable<ImportsWritable> values, Context context) throws IOException, InterruptedException {
			int totalWheatImports = 0;
			int totalMaizeImports = 0;
	    	int totalBarleyImports = 0;
	    	int totalSunflowerOilImports = 0;
	    	
	    	for (ImportsWritable imports : values) {
	    		totalWheatImports += imports.getWheatImports();
	    		totalMaizeImports += imports.getMaizeImports();
	    		totalBarleyImports += imports.getBarleyImports();
	    		totalSunflowerOilImports += imports.getSunflowerOilImports();
	    	}
	    	
	    	ImportsWritable totalImports = new ImportsWritable(totalWheatImports, totalMaizeImports, totalBarleyImports, totalSunflowerOilImports);
	    	
	    	context.write(key, totalImports);
		}
	}
	
	private static class ImportRatiosOutputFormat extends TextOutputFormat<Text, ImportsWritable> {
		private static class ImportRatiosRecordWriter extends RecordWriter<Text, ImportsWritable> {
            private DataOutputStream out;

            ImportRatiosRecordWriter(DataOutputStream stream) {
                this.out = stream;
            }
            
            @Override
            public void write(Text key, ImportsWritable value) throws IOException, InterruptedException {
            	String line = String.format("%-35s\t%9d\t%9d\t%9d\t%9d\t%9d\n",
            			key, value.getWheatImports(), value.getMaizeImports(), value.getBarleyImports(), value.getSunflowerOilImports(), value.getTotalImports());
                out.writeBytes(line);
            }
            
            @Override
            public void close(TaskAttemptContext context) throws IOException, InterruptedException {
                out.close();
            }
		}
		
		@Override
		public RecordWriter<Text, ImportsWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
			Path outputDir = getOutputPath(job);
            Path outputFile = new Path(outputDir, "part-r-00000");
            
            FileSystem fs = outputDir.getFileSystem(job.getConfiguration());
            DataOutputStream out = fs.create(outputFile);

            return new ImportRatiosRecordWriter(out);
		}
	}

    public static void main(String[] args) throws Exception {
    	long startTime = System.nanoTime();
    	
    	Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(ImportsTotal.class);
	    
	    job.setMapperClass(TokenizerMapper.class);
	    job.setReducerClass(ImportSumReducer.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(ImportsWritable.class);
	    job.setOutputFormatClass(ImportRatiosOutputFormat.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    boolean jobStatus = job.waitForCompletion(true);
	    
	    if (!jobStatus) System.exit(1);
	    
	    // Metrics Calculation
	    long endTime = System.nanoTime();
	    double duration = (endTime - startTime) / 1e9d;
	    System.out.printf("Execution Time: %.3f seconds\n", duration);
	    
	    Runtime runtime = Runtime.getRuntime();
	    double usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / (1024.0 * 1024.0);
	    System.out.printf("Memory Used: %.3f MB\n", usedMemory);
	    
	    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
	    double usedHeapMemory = memoryMXBean.getHeapMemoryUsage().getUsed() / (1024.0 * 1024.0);
	    double usedNonHeapMemory = memoryMXBean.getNonHeapMemoryUsage().getUsed() / (1024.0 * 1024.0);
	    System.out.printf("Heap Memory Used: %.3f MB\n", usedHeapMemory);
	    System.out.printf("Non-Heap Memory Used: %.3f MB\n", usedNonHeapMemory);
	    
	    long recordCount = job.getCounters().findCounter(Counter.RECORD_COUNT).getValue();
	    double throughput = recordCount / duration;
	    System.out.printf("Throughput: %.3f records per second\n", throughput);
	    
	    System.exit(0);
    }
}
