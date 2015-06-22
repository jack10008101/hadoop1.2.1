package mapreduce.design.pattern.chap3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Grep is a popular text filtering utility that dates back to Unix and is
 * available on most Unix-like systems. It scans through a file line-by-line and
 * only outputs lines that match a specific pattern. We’d like to parallelize
 * the regular expression search across a larger body of text. In this example,
 * we’ll show how to apply a regular expression to every line in MapReduce.
 * conclusion:As this is a map-only job, there is no combiner or reducer. All
 * output records will be written directly to the file system.
 * 
 * @author jack 2015年6月4日
 *
 */
public class GrepDemo {
	/**
	 * The mapper is pretty straightforward since we use the Java built-in
	 * libra‐ ries for regular expressions. If the text line matches the
	 * pattern, we’ll output the line. Otherwise we do nothing and the line is
	 * effectively ignored. We use the setup function to retrieve the map regex
	 * from the job configuration.
	 * 
	 * @author jack 2015年6月4日
	 *
	 */
	public static class GrepMapper extends
			Mapper<LongWritable, Text, NullWritable, Text> {

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String txt = value.toString();
			String mapRegex = context.getConfiguration().get("mapregex");
			if (value.toString().matches(mapRegex)) {
				context.write(NullWritable.get(), value);
			}
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: DistributedGrep <regex> <in> <out>");
			System.exit(2);
		}
		conf.set("mapregex", otherArgs[0]);

		Job job = new Job(conf, "GrepDemo");
		job.setJarByClass(GrepDemo.class);
		job.setMapperClass(GrepMapper.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0); // Set number of reducers to zero
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
