package Hadoop.inverse.index;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Map阶段 <0,"hello tom"> ....
 * 
 * 
 * context.write("hello->a.txt",1); context.write("hello->a.txt",1);
 * context.write("hello->a.txt",1); context.write("hello->a.txt",1);
 * context.write("hello->a.txt",1);
 * 
 * context.write("hello->b.txt",1); context.write("hello->b.txt",1);
 * context.write("hello->b.txt",1);
 * -------------------------------------------------------- combiner阶段
 * <"hello->a.txt",1> <"hello->a.txt",1> <"hello->a.txt",1> <"hello->a.txt",1>
 * <"hello->a.txt",1>
 * 
 * <"hello->b.txt",1> <"hello->b.txt",1> <"hello->b.txt",1>
 * 
 * context.write("hello","a.txt->5"); context.write("hello","b.txt->3");
 * -------------------------------------------------------- Reducer阶段
 * <"hello",{"a.txt->5","b.txt->3"}>
 * 
 * 
 * context.write("hello","a.txt->5 b.txt->3");
 * ------------------------------------------------------- hello
 * "a.txt->5 b.txt->3" tom "a.txt->2 b.txt->1" kitty "a.txt->1" .......
 * 
 * 倒叙索引
 * 
 * @author jack 2015年5月12日
 *
 */
public class InverseIndex {
	public static class IndexMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		private Text k = new Text();
		private Text v = new Text();

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			String[] words = line.split(" ");
			// 需要知道，词是在哪一个文本中出现的，倒叙索引的格式<documentid,IF>(哪个文档，所属文档的词频)
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String path = fileSplit.getPath().toString();
			for (String string : words) {
				k.set(string + "->" + path);
				v.set("1");
				context.write(k, v);
			}
		}
	}

	public static class IndexConbiner extends Reducer<Text, Text, Text, Text> {
		private Text k = new Text();
		private Text v = new Text();

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] wordAndPath = key.toString().split("->");
			String word = wordAndPath[0];
			String path = wordAndPath[1];
			int counter = 0;
			for (Text text : values) {
				counter += Integer.parseInt(text.toString());
			}
			k.set(word);
			k.set(path + "->" + counter);
			context.write(k, v);
		}
	}

	public static class IndexReducer extends Reducer<Text, Text, Text, Text> {
		private Text v = new Text();

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			StringBuilder sb = new StringBuilder();
			for (Text text : values) {
				sb.append(text.toString()).append("\t");
			}
			v.set(sb.toString());
			context.write(key, v);
		}
	}

	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(InverseIndex.class);
		job.setMapperClass(IndexMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		job.setCombinerClass(IndexConbiner.class);
		job.setReducerClass(IndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
	}
}
