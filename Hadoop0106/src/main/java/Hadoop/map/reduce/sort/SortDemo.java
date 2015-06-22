package Hadoop.map.reduce.sort;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 排序，map自带排序的程序，按照map输出的key值排序，map输出的key值若是string类型按照字典顺序，若是int类型按照数字的大小排序
 * 
 * @author jack 2015年5月17日
 *
 */
public class SortDemo {
	public static class SortMapper extends
			Mapper<LongWritable, Text, IntWritable, IntWritable> {
		private IntWritable k = new IntWritable();
		private IntWritable v = new IntWritable(1);

		@Override
		protected void map(
				LongWritable key,
				Text value,
				Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			k.set(Integer.parseInt(line));
			context.write(k, v);
		}
	}

	/**
	 * reducer是将输入key的值复制到输出的value，然后根据输入的value-list中元素的个数决定输出的出次数， 用全局
	 * 
	 * @author jack 2015年5月17日
	 *
	 */
	public static class SortReducer extends
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		private IntWritable lineNumber = new IntWritable(1);

		@Override
		protected void reduce(
				IntWritable key,
				Iterable<IntWritable> value,
				Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
            for (IntWritable intWritable : value) {
				context.write(lineNumber, key);
				lineNumber=new IntWritable(lineNumber.get()+1);
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job=Job.getInstance(new Configuration());
        job.setJarByClass(SortDemo.class);
        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
	}
}
