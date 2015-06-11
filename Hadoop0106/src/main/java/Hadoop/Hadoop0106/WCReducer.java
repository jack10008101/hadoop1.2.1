package Hadoop.Hadoop0106;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	@Override
	protected void reduce(Text arg0, Iterable<LongWritable> arg1,
			Reducer<Text, LongWritable, Text, LongWritable>.Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//定义一个计数器
		long counter=0;
		//循环Iterable<LongWritable> arg1
		for (LongWritable longWritable : arg1) {
			counter+=longWritable.get();
		}
		//输出
		arg2.write(arg0, new LongWritable(counter));
	}
}
