package mapreduce.design.pattern.chap3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

/**
 * 布隆过滤器 生成二进制文件数据
 * 总结：对于这些需要实现训练的算法，首先是使用训练集来训练出算法，然后将训练后的对象写入HDFS或者其他的文件中
 * 然后在使用的时候需要去反序列化这个对象。在bloom filter的实例中，将训练好的对象写入HDFS中，然后在mapreduce
 * 需要使用的时候反序列化得到对象。
 * @author jack 2015年6月6日
 *
 */
public class TrainingBloomData {
	/**
	 * 根据记录的大小和假正的概率来确定布隆过滤器的数组的大小 而对于给定的False Positives概率 p，如何选择最优的位数组大小 m
	 * 
	 * @param numRecords
	 * @param falsePosRate
	 * @return
	 */
	public static int getOptimalBloomFilterSize(int numRecords,
			float falsePosRate) {
		int size = (int) (-numRecords * (float) Math.log(falsePosRate) / Math
				.pow(Math.log(2), 2));
		return size;
	}

	/**
	 * K=(m/n)ln2 布隆过滤器假正例概率 p 与位数组大小 m 和集合中插入元素个数 n 的关系图，假定 Hash 函数个数选取最优数目
	 * 
	 * @param numMembers
	 *            ,输入的n个元素
	 * @param vectorSize
	 *            ,位数组的大小
	 * @return
	 */
	public static int getOptimalK(float numMembers, float vectorSize) {
		return (int) Math.round(vectorSize / numMembers * Math.log(2));
	}

	public static void main(String[] args) throws IOException {
		Path inputPath = new Path(args[0]);// 输入在hdfs中输入的训练集合数据
		int numMenbers = Integer.parseInt("10");
		float falsePosRate = Float.parseFloat("0.01");
		Path bfFile = new Path("hdfs://192.168.0.16:9000/root/bloom/bloom.bin"); // 将训练得到的数据写入如下文件路径
		FileSystem fss = FileSystem.get(new Configuration());
		
		if (fss.exists(bfFile)) {
			fss.delete(bfFile, true);
			System.out.println("存在此路径，已经删除！");
		}
		// 计算在给定的输入的元素数和假正率，计算得到hash函数映射到的位数组的大小和哈希函数的个数
		int vectorSize = getOptimalBloomFilterSize(numMenbers, falsePosRate);
		int nbHash = getOptimalK(numMenbers, vectorSize);
		// create new Bloom filter
		BloomFilter filter = new BloomFilter(vectorSize, nbHash,
				Hash.MURMUR_HASH);
		// open file for read
		System.out.println("Training Bloom filter of size" + vectorSize
				+ "with" + nbHash + "hash functions," + numMenbers
				+ "approximate number of records, and " + falsePosRate
				+ "false positive rate");
		String line = null;
		int numRecords = 0;
		FileSystem fs = FileSystem.get(new Configuration());
		for (FileStatus status : fs.listStatus(inputPath)) {
			BufferedReader rdr = null;
			// if file is gzipped, wrap it in a GZIPInputStream
			if (status.getPath().getName().endsWith(".gz")) {
				rdr = new BufferedReader(new InputStreamReader(
						new GZIPInputStream(fs.open(status.getPath()))));
			} else {
				rdr = new BufferedReader(new InputStreamReader(fs.open(status
						.getPath())));
			}
			System.out.println("Reading...." + status.getPath());
			while ((line = rdr.readLine()) != null) {
				filter.add(new Key(line.getBytes()));
				numRecords++;
			}
			rdr.close();
		}
		System.out.println("Trained Bloom filter with " + numRecords

		+ " entries.");
		System.out.println("Serializing Bloom filter to HDFS at " + bfFile);
		FSDataOutputStream strm = fs.create(bfFile);
		filter.write(strm);
		strm.flush();
		strm.close();
		System.out.println("Done training Bloom filter.");
	}
}
