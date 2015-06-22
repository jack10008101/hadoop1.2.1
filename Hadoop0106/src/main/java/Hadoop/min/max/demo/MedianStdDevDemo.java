package Hadoop.min.max.demo;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import Hadoop.utils.MapUtil;

/**
 * Given a list of user’s comments, determine the median and standard deviation
 * of comment lengths per hour of day. 计算评论长度的中值
 * 
 * @author jack 2015年5月29日
 *
 */
public class MedianStdDevDemo {
	/**
	 * The mapper will process each input record to calculate the median comment
	 * length within each hour of the day. The output key is the hour of day,
	 * which is parsed from the CreationDate XML attribute. The output value is
	 * a single value: the comment length.
	 * 
	 * @author jack 2015年5月29日
	 *
	 */
	public static class MedianStdDevMapper extends
			Mapper<IntWritable, Text, IntWritable, IntWritable> {
		private IntWritable outHour = new IntWritable();
		private IntWritable outCommentLength = new IntWritable();
		private final static SimpleDateFormat frmt = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ss.SSS");

		@SuppressWarnings("deprecation")
		@Override
		protected void map(
				IntWritable key,
				Text value,
				Mapper<IntWritable, Text, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Map<String, String> parsed = MapUtil.transformXmlToMap(value
					.toString());
			// Grab the "CreationDate" field,
			// since it is what we are grouping by
			String strDate = parsed.get("CreationDate");
			// Grab the comment to find the length
			String text = parsed.get("Text");
			// get the hour this comment was posted in
			try {
				Date creationDate = frmt.parse(strDate);
				outHour.set(creationDate.getHours());
				// set the comment length
				outCommentLength.set(text.length());
				// write out the user ID with min max dates and
				// count,map的输出：小时是输出的key，评论的长度是输出的value
				context.write(outHour, outCommentLength);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * The reducer code iterates through the given set of values and adds each
	 * value to an in-memory list. The iteration also calculates a running sum
	 * and count. After iteration, the comment lengths are sorted to find the
	 * median value. If the list has an odd number of entries, the median value
	 * is set to the middle value. If the number is even, the middle two values
	 * are averaged. Next, the standard deviation is calculated by iter‐ ating
	 * through our sorted list after finding the mean from our running sum and
	 * count. A running sum of deviations is calculated by squaring the
	 * difference between each comment length and the mean. The standard
	 * deviation is then calculated from this sum. Finally, the median and
	 * standard deviation are output along with the input key.
	 * 
	 * @author jack 2015年5月31日
	 *
	 */
	public static class MedianStdDevRedecuer extends
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		private MedianStdDevTuple result = new MedianStdDevTuple();
		private ArrayList<Float> commentLengths = new ArrayList<Float>();

		@Override
		protected void reduce(
				IntWritable key,
				Iterable<IntWritable> values,
				Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			float sum = 0;
			float count = 0;
			commentLengths.clear();
			result.setStdDev(0);
			// Iterate through all input values for this key
			for (IntWritable val : values) {
				commentLengths.add((float) val.get());
				sum += val.get();
				count++;
			}
			// sort commentLengths to calculate median
			Collections.sort(commentLengths);
			// if commentLengths is an even value, average middle two elements
			if (count % 2 == 0) {
				result.setMedian((commentLengths.get((int) count / 2 - 1) + commentLengths
						.get((int) count / 2)) / 2.0f);
			} else {
				result.setMedian(commentLengths.get((int) count / 2));
			}
			// calculate standard deviation
			float mean = sum / count;
			float sumOfSquares = 0.0f;
			for (Float f : commentLengths) {
				sumOfSquares += (f - mean) * (f - mean);
			}
			result.setStdDev((float) Math.sqrt(sumOfSquares / (count - 1)));

		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
        
	}

}
