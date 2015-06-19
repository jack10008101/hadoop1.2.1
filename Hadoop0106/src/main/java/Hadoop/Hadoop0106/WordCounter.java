package Hadoop.Hadoop0106;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 1.分析具体的业务逻辑，确定输入输出的key，value的格式
 * 2.自定义一个类继承org.apache.hadoop.mapreduce.Mapper，重写其中map方法，实现自己的业务逻辑，输出key，value
 * 3.自定义一个类继承org.apache.hadoop.mapreduce.Reducer，重写其中reduce方法，实现自己的业务逻辑，输出key，value
 * 4.将自定义的Mapper和Reducer对象通过job对象组装起来
 * @author jack 2015年4月16日
 *
 */
public class WordCounter {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		// 获得job对象，并将我们自定义map，reduce组装
		Job job = new Job(new Configuration());
		// 注意将main方法所在类的.class文件写入
		job.setJarByClass(WordCounter.class);
		// 设置map相关的属性
		job.setMapperClass(WCMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		// 在hdfs上面读取数据
		FileInputFormat.setInputPaths(job, new Path("/words.txt"));
		// 设置reduce相关的属性
		job.setReducerClass(WCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path("/wccount150416"));
		// 提交任务
		job.waitForCompletion(true);

	}

}
