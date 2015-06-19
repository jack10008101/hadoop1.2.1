package Hadoop.Hadoop0106;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.map(key, value, context);
		//1接收数据value
		String line=value.toString();
		//切割数据
		String[] worgs=line.split(" ");
		//循环
		for (String word : worgs) {
			//出现一次，记一个，并输出
			context.write(new Text(word), new LongWritable(1));
		}
	}
}
