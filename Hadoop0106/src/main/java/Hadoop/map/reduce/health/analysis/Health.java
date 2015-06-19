package Hadoop.map.reduce.health.analysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.tools.ant.filters.TokenFilter.StringTokenizer;

/**
 * 
 * @author jack
 *  2015年5月17日
 *
 */
public class Health {
    public static class HealthMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    	@Override
    	protected void map(LongWritable key, Text value,
    			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
    			throws IOException, InterruptedException {
    		// TODO Auto-generated method stub
    		//super.map(key, value, context);
    	}
    }
    public static class HealthReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    	@Override
    	protected void reduce(Text key, Iterable<IntWritable> values,
    			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
    			throws IOException, InterruptedException {
    		// TODO Auto-generated method stub
    		StringTokenizer tokenizer=new StringTokenizer();
    		
    	}
    }
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
