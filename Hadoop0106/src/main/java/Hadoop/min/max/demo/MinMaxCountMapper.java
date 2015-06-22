package Hadoop.min.max.demo;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * The output key is the user ID and the value is three columns of our future
 * output: the minimum date, the maximum date, and the number of comments this
 * user has created
 * 
 * @author jack 2015年5月28日
 *
 */
public class MinMaxCountMapper extends
		Mapper<IntWritable, Text, Text, MinMaxCountTuple> {
	// Our output key and value Writables
	private Text outUserId = new Text();
	private MinMaxCountTuple outTuple = new MinMaxCountTuple();
	// This object will format the creation date string into a Date object
	private final static SimpleDateFormat frmt = new SimpleDateFormat(
			"yyyy-MM-dd'T'HH:mm:ss.SSS");

	@Override
	protected void map(IntWritable key, Text value,
			Mapper<IntWritable, Text, Text, MinMaxCountTuple>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Map<String, String> parsed = transformXmlToMap(value.toString());
		// Grab the "CreationDate" field since it is what we are finding
		// the min and max value of
		String strDate = parsed.get("CreationDate");
		// Grab the “UserID” since it is what we are grouping by
		String userId = parsed.get("UserId");
		// Parse the string into a Date object
		try {
			Date creationDate = frmt.parse(strDate);
			// Set the minimum and maximum date values to the creationDate
			outTuple.setMin(creationDate);
			outTuple.setMax(creationDate);
			// Set the comment count to 1
			outTuple.setCount(1);
			// Set our user ID as the output key
			outUserId.set(userId);
			context.write(outUserId, outTuple);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	// This helper function parses the stackoverflow into a Map for us.
	public  Map<String, String> transformXmlToMap(String xml) {
		Map<String, String> map = new HashMap<String, String>();
		try {
			// exploit the fact that splitting on double quote
			// tokenizes the data nicely for us
			String[] tokens = xml.trim().substring(5, xml.trim().length() - 3)
					.split("\"");
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
