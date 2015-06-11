package mapreduce.design.pattern.chap3;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Implementing SRS as a filter operation is not a direct application of the
 * filtering pattern, but the structure is the same. Instead of some filter
 * criteria function that bears some relationship to the content of the record,
 * a random number generator will produce a value, and if the value is below a
 * threshold, keep the record. Otherwise, toss it out.
 * 
 * @author jack 2015年6月4日
 *
 */
public class SimpleRandomSampling {
	/**
	 * In the mapper code, the setup function is used to pull the filter_per
	 * centage configuration value so we can use it in the map function. In the
	 * map function, a simple check against the next random number is done. The
	 * ran‐ dom number will be anywhere between 0 and 1, so by comparing against
	 * the specified threshold, we can keep or throw out the record.
	 * 
	 * @author jack 2015年6月4日 As this is a map-only job, there is no combiner or
	 *         reducer. All output records will be written directly to the file
	 *         system. When using a small percentage, you will find that the
	 *         files will be tiny and plentiful. If this is the case, set the
	 *         number of reducers to 1 without specifying a reducer class, which
	 *         will tell the MapReduce framework to use a single identity
	 *         reducer that simply collects the output into a single file. The
	 *         other option would be to collect the files as a post-processing
	 *         step using hadoop fs -cat.
	 */
	public static class SRSMapper extends
			Mapper<LongWritable, Text, NullWritable, Text> {
		private Random rands = new Random();
		private Double percentage;

		@Override
		protected void setup(
				Mapper<LongWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// Retrieve the percentage that is passed in via the configuration
			// like this: conf.set("filter_percentage", .5);
			// for .5%
			String strPercentage = context.getConfiguration().get(
					"filter_percentage");
			percentage = Double.parseDouble(strPercentage) / 100.0;
		}

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			if (rands.nextDouble() < percentage) {
				context.write(NullWritable.get(), value);
			}
		}
	}

	public static void main(String[] args) {
		/*
		 * As this is a map-only job, there is no combiner or reducer. All
		 * output records will be written directly to the file system. When
		 * using a small percentage, you will find that the files will be tiny
		 * and plentiful. If this is the case, set the number of reducers to 1
		 * without specifying a reducer class, which will tell the MapReduce
		 * framework to use a single identity reducer that simply collects the
		 * output into a single file. The other option would be to collect the
		 * files as a post-processing step using hadoop fs -cat.
		 */
	}
}
