package mapreduce.design.pattern.chap3;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

/**
 * ReduceJoin+布隆过滤器 实现Reduce Join
 * 
 * @author jack 2015年6月6日
 *
 */
public class ReduceBoolmFilter {
	/**
	 * 自定义一个输出实体
	 * 
	 * @author jack 2015年6月6日
	 *
	 */
	public static class CombineEntity implements
			WritableComparable<CombineEntity> {
		private Text joinKey;// 连接KEY
		private Text flag;// 文件来源标识
		private Text secondPart;// 除了键外的其他部分的数据

		public CombineEntity() {
			this.joinKey = new Text();
			this.flag = new Text();
			this.secondPart = new Text();
		}

		public Text getJoinKey() {
			return joinKey;
		}

		public void setJoinKey(Text joinKey) {
			this.joinKey = joinKey;
		}

		public Text getFlag() {
			return flag;
		}

		public void setFlag(Text flag) {
			this.flag = flag;
		}

		public Text getSecondPart() {
			return secondPart;
		}

		public void setSecondPart(Text secondPart) {
			this.secondPart = secondPart;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			this.joinKey.write(out);
			this.flag.write(out);
			this.secondPart.write(out);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			this.joinKey.readFields(in);
			this.flag.readFields(in);
			this.secondPart.readFields(in);
		}

		@Override
		public int compareTo(CombineEntity o) {
			// TODO Auto-generated method stub
			return this.joinKey.compareTo(o.joinKey);
		}

	}

	private static class JMapper extends
			Mapper<LongWritable, Text, Text, CombineEntity> {
		private CombineEntity combine = new CombineEntity();
		private Text joinKey = new Text();
		private Text flag = new Text();
		private Text secondPart = new Text();
		/**
		 * 使用布隆过滤器存储key 代替原来的HashSet存储
		 * */
		BloomFilter filter = new BloomFilter();// 实施反序列化

		/*
		 * the Bloom filter is deserialized from the DistributedCache before
		 * being used in the map method
		 */

		@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, CombineEntity>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// 读取文件流
			BufferedReader bf = null;
			String temp;
			// 获取DistributedCached里面 的共享文件
			Path[] paths = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());
			for (Path path : paths) {
				if (path.toString().contains("bloom.bin")) {
					DataInputStream strm = new DataInputStream(
							new FileInputStream(path.toString()));
					// Read into our Bloom filter.
					filter.readFields(strm);
					strm.close();
				}
			}
		}

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, CombineEntity>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// 获得文件输入路径
			String pathName = ((FileSplit) context.getInputSplit()).getPath()
					.toString();
			if (pathName.endsWith("a.txt")) {
				String[] valueItems = value.toString().split(",");
				/**
				 * 在这里过滤必须要的连接字符
				 * 
				 * */
				if (filter.membershipTest(new Key(valueItems[0].getBytes()))) {
					// 设置标志位
					flag.set("0");
					// 设置链接键
					joinKey.set(valueItems[0]);
					// 设置第二部分
					secondPart.set(valueItems[1] + "\t" + valueItems[2]);
					// 封装实体
					combine.setJoinKey(joinKey);
					combine.setFlag(flag);
					combine.setSecondPart(secondPart);
					context.write(joinKey, combine);
				} else {
					System.out.println("a.txt里");
					System.out.println("在小表中无此记录，执行过滤掉！");
					for (String v : valueItems) {
						System.out.print(v + "   ");
					}

					return;
				}
			} else if (pathName.endsWith("b.txt")) {
				String[] valueItems = value.toString().split(",");
				/**
				 * 
				 * 判断是否在集合中
				 * 
				 * */
				if (filter.membershipTest(new Key(valueItems[0].getBytes()))) {
					// 设置标志位
					flag.set("1");
					// 设置链接键
					joinKey.set(valueItems[0]);
					// 设置第二部分
					secondPart.set(valueItems[1] + "\t" + valueItems[2] + "\t"
							+ valueItems[3]);
					// 封装实体
					combine.setJoinKey(joinKey);
					combine.setFlag(flag);
					combine.setSecondPart(secondPart);
					context.write(joinKey, combine);
				} else {
					System.out.println("b.txt里");
					System.out.println("在小表中无此记录，执行过滤掉！");
					for (String v : valueItems) {
						System.out.print(v + "   ");
					}

					return;

				}
			}
		}
	}

	public static class JReduce extends
			Reducer<Text, CombineEntity, Text, Text> {
		private List<Text> lefTable = new ArrayList<Text>();
		private List<Text> rightTable = new ArrayList<Text>();
		private Text secondPart = null;
		private Text outPut = new Text();

		@Override
		protected void reduce(Text key, Iterable<CombineEntity> values,
				Reducer<Text, CombineEntity, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			lefTable.clear();
			rightTable.clear();
			/**
			 * 将不同文件的数据，分别放在不同的集合 中，注意数据量过大时，会出现 OOM的异常
			 * **/
			for (CombineEntity combineEntity : values) {
				this.secondPart = new Text(combineEntity.getSecondPart()
						.toString());
				// left tables
				if ("0".equals(combineEntity.getFlag().toString().trim())) {
					lefTable.add(secondPart);
				} else if ("1"
						.equals(combineEntity.getFlag().toString().trim())) {
					rightTable.add(secondPart);
				}
			}
			for (Text left : lefTable) {
				for (Text right : rightTable) {
					outPut.set(left + "\t" + right);
					context.write(key, outPut);
				}
			}
		}

	}

	public static void main(String[] args) throws IOException,
			URISyntaxException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		job.setJarByClass(ReduceBoolmFilter.class);
		DistributedCache.addCacheFile(new URI(
				"hdfs://192.168.75.130:9000/root/bloom/bloom.bin"),
				configuration);
        job.setMapperClass(JMapper.class);
        job.setReducerClass(JReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CombineEntity.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileSystem fs=FileSystem.get(configuration);
        Path op=new Path("hdfs://192.168.0.16:9000/root/outputjoindbnew5"); 
        if (fs.exists(op)) {
			fs.delete(op, true);
			System.out.println("存在此输出路径，已删除！！！");
		}
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, op);
        System.exit(job.waitForCompletion(true)?0:1);
	}

}
