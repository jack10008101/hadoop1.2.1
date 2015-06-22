package Hadoop.map.reduce.table.join;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 表的自身连接：
 * 表格的形式如下:
 * child parent
 * tom lucy
 * tom jack
 * jone lucy
 * jone jack
 * lucy mary
 * lucy ben
 * 表的自身连接：将表的parent和child连接。具体操作，形成两张相同的表a和表b,将表a中的parent列和表b中的child
 * 连接，最后可以得到grandchild和grandparent的表的关系。我们需要两个map输出，一个是按照表a中的parent列为key值输出
 * 另一个是按照表b中的child列为i输出key值，在map的输出是会按照相同的key聚集在一起，然后将map的两个输出按照key连接
 * 同时需要在输出的时候做出标记表明是表a还是表b.
 * @author jack
 *  2015年5月17日
 *
 */
public class STJoin {
	public static int num=0;
	public static class STMapper extends Mapper<LongWritable, Text, Text, Text>{
		private Text k=new Text();
		private Text v=new Text();
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String childName=null;
			String parentName=null;
			String relationTypeString=null;
			String line=value.toString();
			String []splits=line.split(" ");
			if (!"child".equals(splits[0])) {
				childName=splits[0];
				parentName=splits[1];
				relationTypeString="1";//左右表的区分
				//表a
				k.set(parentName);
				v.set(relationTypeString+"+"+childName+"+"+parentName);
				context.write(k, v);
				//表b
				relationTypeString="2";
				k.set(childName);
				v.set(relationTypeString+"+"+childName+"+"+parentName);
				context.write(k, v);
			}
		}
	}
	public static class STReducer extends Reducer<Text, Text, Text, Text>{
		private Text k=new Text();
		private Text v=new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			if(num==0){
				k.set("grandChild");
				v.set("grandParent");
				context.write(k, v);
				num++;
			}
			String [] grandChildren=new String[10];
			int grandChildIndex=0;
			String [] grandParents=new String [10];
			int grandParentIndex=0;
           Iterator iterator= values.iterator();
				while (iterator.hasNext()) {
					String recoder=iterator.next().toString();
					int len=recoder.length();
					int i=2;
					if (len==0) {
						continue;
					}
					char relationType=recoder.charAt(0);
					String childName=null;
					String parentName=null;
					//获取value-list中的value的child
					while (recoder.charAt(i)!='+') {
						childName=childName+recoder.charAt(i);
					    i++;
					}
					i=i+1;
					//获取value-list中的value的parent
					while (i<len) {
						parentName=parentName+recoder.charAt(i);
						i++;
					}
					if (relationType=='1') {
						grandChildren[grandChildIndex++]=childName;
					}else {
						grandParents[grandParentIndex++]=parentName;
					}
				}
			   //遍历输出笛卡尔积
				if (grandChildIndex!=0&& grandParentIndex!=0) {
					for (int m = 0; m < grandChildIndex; m++) {
						for (int n = 0; n < grandParentIndex;n++) {
							k.set(grandChildren[m]);
							v.set(grandParents[n]);
							context.write(k, v);
						}
					}
				}
		}
	}
public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	
	
	Job job=Job.getInstance(new Configuration());
	job.setJarByClass(STJoin.class);
	job.setMapperClass(STMapper.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
	FileInputFormat.setInputPaths(job, new Path(args[0]));
	job.setReducerClass(STReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	job.waitForCompletion(true);
     }
  }
