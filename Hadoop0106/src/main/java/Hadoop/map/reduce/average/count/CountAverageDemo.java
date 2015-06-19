package Hadoop.map.reduce.average.count;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The mapper will process each input record to calculate the average comment
 * length based on the time of day. The output key is the hour of day,The output
 * value is two columns, the comment count and the average length of the
 * comments for that hour
 * 
 * @author jack 2015年5月28日
 *
 */
public class CountAverageDemo {
	public static class AverageMapper extends
			Mapper<IntWritable, Text, IntWritable, CountAverageTuple> {
		private IntWritable outHour = new IntWritable();
		private CountAverageTuple outCountAverage = new CountAverageTuple();
		private static final SimpleDateFormat frmt = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ss.SSS");

		@SuppressWarnings("deprecation")
		@Override
		protected void map(
				IntWritable key,
				Text value,
				Mapper<IntWritable, Text, IntWritable, CountAverageTuple>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Map<String, String> parsed = transformXmlToMap(value.toString());
			// Grab the "CreationDate" field,
			// since it is what we are grouping by
			String strDate = parsed.get("CreationDate");
			// Grab the comment to find the length
			String text = parsed.get("Text");
			// get the hour this comment was posted in
			try {
				Date craetionDate = frmt.parse(strDate);
				outHour.set(craetionDate.getHours());
				// get the comment length
				outCountAverage.setCount(1);
				outCountAverage.setAverage(text.length());
				// write out the hour with the comment length
				context.write(outHour, outCountAverage);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		/**
		 * The reducer code iterates through all given values for the hour and
		 * keeps two local variables: a running count and running sum
		 * 
		 * @author jack 2015年5月28日
		 *
		 */
		public static class AverageReducer
				extends
				Reducer<IntWritable, CountAverageTuple, IntWritable, CountAverageTuple> {
			private CountAverageTuple result = new CountAverageTuple();

			@Override
			protected void reduce(
					IntWritable key,
					Iterable<CountAverageTuple> values,
					Reducer<IntWritable, CountAverageTuple, IntWritable, CountAverageTuple>.Context context)
					throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				float sum = 0;
				float count = 0;
				// Iterate through all input values for this key
				for (CountAverageTuple val : values) {
					sum += val.getAverage() * val.getCount();
					count += val.getCount();
				}
				result.setCount(count);
				result.setAverage(sum/count);
				context.write(key, result);
			}
		}

		// This helper function parses the stackoverflow into a Map for us.
		public Map<String, String> transformXmlToMap(String xml) {
			Map<String, String> map = new HashMap<String, String>();
			try {
				// exploit the fact that splitting on double quote
				// tokenizes the data nicely for us
				String[] tokens = xml.trim()
						.substring(5, xml.trim().length() - 3).split("\"");
				for (int i = 0; i < tokens.length - 1; i += 2) {
					String key = tokens[i].trim();
					String val = tokens[i + 1];
					map.put(key.substring(0, key.length() - 1), val);
				}
			} catch (StringIndexOutOfBoundsException e) {
				System.err.println(xml);
			}
			return map;
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
        
	}

}
