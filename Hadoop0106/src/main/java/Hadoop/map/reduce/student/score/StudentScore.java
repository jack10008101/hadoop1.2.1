package Hadoop.map.reduce.student.score;

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
 * 统计学生的平均成绩，每行的数据表示学生姓名和他相应的一门课的成绩，若有多门课就有多条记录
 * 
 * @author jack 2015年5月17日
 *
 */
public class StudentScore {
	public static class StudentMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// super.map(key, value, context);
			Text k = new Text();
			IntWritable v = new IntWritable();
			String line = value.toString();// 讲输入的每一行数据转化为string类型便于处理
			String[] lines = line.split(" ");
			int score = Integer.parseInt(lines[1]);
			k.set(lines[0]);
			v.set(score);
			context.write(k, v);
		}
	}

	public static class StudentReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> value,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			IntWritable v = new IntWritable();
			int sum = 0;
			int counter = 0;
			for (IntWritable intWritable : value) {
				sum += intWritable.get();
				counter++;
			}
			int average = sum / counter;// average score
			v.set(average);
			context.write(key, v);
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(StudentScore.class);
		job.setMapperClass(StudentMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		job.setReducerClass(StudentReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}
