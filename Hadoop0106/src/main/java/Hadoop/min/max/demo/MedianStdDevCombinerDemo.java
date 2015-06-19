package Hadoop.min.max.demo;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import Hadoop.utils.MapUtil;

/**
 * Problem: Given a list of user’s comments, determine the median and standard
 * deviation of comment lengths per hour of day.
 * 
 * @author jack 2015年5月31日
 *
 */
public class MedianStdDevCombinerDemo {
	/**
	 * The mapper processes each input record to calculate the median comment
	 * length based on the hour of the day during which the comment was posted.
	 * The output key is the hour of day, which is parsed from the creation date
	 * XML attribute. The output value is a SortedMapWritable object that
	 * contains one element: the comment length and a count of “1”. This map is
	 * used more heavily in the combiner and reducer.
	 * 
	 * @author jack 2015年5月31日
	 *
	 */
	public static class MedianStdDevCombinerMapper extends
			Mapper<IntWritable, Text, IntWritable, SortedMapWritable> {
		private IntWritable commentLength = new IntWritable();
		private static final LongWritable ONE = new LongWritable(1);
		private IntWritable outHouer = new IntWritable();
		private final static SimpleDateFormat frmt = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ss.SSS");

		@SuppressWarnings("deprecation")
		@Override
		protected void map(
				IntWritable key,
				Text value,
				Mapper<IntWritable, Text, IntWritable, SortedMapWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Map<String, String> parsed = MapUtil.transformXmlToMap(value
					.toString());
			// Grab the "CreationDate" field,
			// since it is what we are grouping by
			String strDate = parsed.get("CreationDate");
			// Grab the comment to find the length
			String text = parsed.get("Text");
			// Get the hour this comment was posted in
			try {
				Date creationDate = frmt.parse(strDate);
				outHouer.set(creationDate.getHours());
				commentLength.set(text.length());
				SortedMapWritable sortedMapWritable = new SortedMapWritable();// 将评论的长度作为KEY
				sortedMapWritable.put(commentLength, ONE);// value值默认是一，这是在map阶段，在combiner或者reducer阶段会累加
				// Write out the user ID with min max dates and count
				context.write(outHouer, sortedMapWritable);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	/**
	 * The reducer code iterates through the given set of SortedMapWritable to
	 * aggregate all the maps together into a single TreeMap, which is a
	 * implementation of SortedMap. The key is the comment length and the value
	 * is the total count associated with the comment length.
	 * 
	 * @author jack 2015年6月1日
	 *
	 */
	public static class MedianStdDevCombinerReducer
			extends
			Reducer<IntWritable, SortedMapWritable, IntWritable, MedianStdDevTuple> {
		private MedianStdDevTuple result = new MedianStdDevTuple();
		private TreeMap<Integer, Long> commentLengthCounts = new TreeMap<Integer, Long>();

		@Override
		protected void reduce(
				IntWritable key,
				Iterable<SortedMapWritable> values,
				Reducer<IntWritable, SortedMapWritable, IntWritable, MedianStdDevTuple>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			float sum = 0;
			long totalComents = 0;// 用于记录当前的小时的所有的评论数
			commentLengthCounts.clear();
			result.setMedian(0);
			result.setStdDev(0);
			for (SortedMapWritable v : values) {
				for (Entry<WritableComparable, Writable> entry : v.entrySet()) {
					int length = ((IntWritable) entry.getKey()).get();// 返回存储在SortedMapWritable中的KEY值即为，即为评论的长度
					long count = ((LongWritable) entry.getKey()).get();// 传递到reducer阶段的对于当前每个小时中的相同评论长度的计数
					totalComents += count;
					sum += length * count;
					Long storedCount = commentLengthCounts.get(length);// if the
																		// key
																		// length
																		// exists
																		// in
																		// the
																		// treemap
																		// then
																		// return
																		// the
																		// value,otherwise
																		// return
																		// null
					if (storedCount == null) {// if the commentLengthCounts do
												// not exist the key,then insert
						commentLengthCounts.put(length, count);
					} else {
						commentLengthCounts.put(length, storedCount + count);
					}

				}
			}
			long medianIndex = totalComents / 2L;
			long previousComments = 0;
			long comments = 0;
			int prevKey = 0;
			/**
			 * The entry set of the Tree Map is then iterated to find the keys
			 * that satisfy the condition previousCommentCount ≤ medianIndex <
			 * commentCount, adding the value of the tree map to comments at
			 * each step of the iteration.
			 */
			for (Entry<Integer, Long> entry : commentLengthCounts.entrySet()) {
				comments = previousComments + entry.getValue();
				if (previousComments <= medianIndex && medianIndex < comments) {
					if (totalComents % 2 == 0
							&& previousComments == medianIndex) {
						result.setMedian((entry.getKey() + prevKey) / 2.0f);
					} else {
						result.setMedian(entry.getKey());
					}
					break;
				}
				previousComments = comments;
				prevKey = entry.getKey();
			}
			// calculate standard deviation
			float mean = sum / totalComents;
			float sumOfSquares = 0.0f;
			for (Entry<Integer, Long> entry : commentLengthCounts.entrySet()) {
				sumOfSquares += (entry.getKey() - mean)
						* (entry.getKey() - mean) * entry.getValue();
			}
			result.setStdDev((float) Math.sqrt(sumOfSquares)
					/ (totalComents - 1));
			context.write(key, result);
		}
	}

	/**
	 * the combiner for this algorithm is different from the reducer. While the
	 * reducer actually calculates the median and stan‐ dard deviation, the
	 * combiner aggregates the SortedMapWritable entries for each local map’s
	 * intermediate key/value pairs. The code to parse through the entries and
	 * aggregate them in a local map is identical to the reducer code in the
	 * previous section. Here, a HashMap is used instead of a TreeMap, because
	 * sorting is unnecessary and a HashMap is typically faster. While the
	 * reducer uses this map to calculate the median and standard deviation, the
	 * combiner uses a SortedMapWritable in order to serialize it for the reduce
	 * phase.
	 * 
	 * @author jack 2015年6月1日
	 *
	 */
	public static class MedianStdDevCombiner
			extends
			Reducer<IntWritable, SortedMapWritable, IntWritable, SortedMapWritable> {
		@Override
		protected void reduce(
				IntWritable key,
				Iterable<SortedMapWritable> values,
				Reducer<IntWritable, SortedMapWritable, IntWritable, SortedMapWritable>.Context context)
				throws IOException, InterruptedException {
			SortedMapWritable outValue = new SortedMapWritable();
			// values里面有着大量的SortedMapWritable组成的元素，需要一一遍历
			for (SortedMapWritable v : values) {
				for (Entry<WritableComparable, Writable> entry : v.entrySet()) {
					// 先查询在当前的outValue中是否存在当前的KEY，如果存在取出已有的值加上当前的值设置为当前KEY值对应的value
					// 如果没有则将当前的KEY和value放入outValue中
					LongWritable count = (LongWritable) outValue.get(entry
							.getKey());
					if (count != null) {
						outValue.put(
								entry.getKey(),
								new LongWritable(((LongWritable) entry
										.getValue()).get()
										+ ((LongWritable) outValue.get(entry
												.getKey())).get()));
					} else {
						outValue.put(entry.getKey(), new LongWritable(
								((LongWritable) entry.getValue()).get()));
					}
				}
			}
			context.write(key, outValue);
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
        
	}

}
