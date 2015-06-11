package mapreduce.design.pattern.chap2;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import Hadoop.utils.MapUtil;

/**
 * The mapper parses the posts from StackOverflow to output the row IDs of all
 * answer posts that contain a particular Wikipedia URL. First, the XML
 * attributes for the text, post type, and row ID are extracted. If the post
 * type is not an answer, identified by a post type of “2”, we parse the text to
 * find a Wikipedia URL. This is done using the getWikipediaURL method, which
 * takes in a String of unescaped HTML and returns a Wikipedia URL if found, or
 * null otherwise. The method is omitted for brevity. If a URL is found, the URL
 * is output as the key and the row ID is output as the value.
 * 
 * @author jack 2015年6月3日
 *
 */
public class WikipediaExtractor extends Mapper<LongWritable, Text, Text, Text> {
	private Text link = new Text();
	private Text outKey = new Text();

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Map<String, String> parsed = MapUtil
				.transformXmlToMap(value.toString());
		// Grab the necessary XML attributes
		String txt = parsed.get("Body");
		String postType = parsed.get("PostTypeId");
		String row_id = parsed.get("Id");
		// if the body is null, or the post is a question (1), skip
		if (txt == null || (postType != null && postType.equals("1"))) {
			return;
		}
		txt = StringEscapeUtils.unescapeHtml(txt.toLowerCase());
		link.set(getWikipediaURL(txt));
		outKey.set(row_id);
		context.write(link, outKey);
	}

	/**
	 * The reducer iterates through the set of input values and appends each row
	 * ID to a String, delimited by a space character. The input key is output
	 * along with this concatenation. Combiner optimization. The combiner can be
	 * used to do some concatenation prior to the reduce phase. Because all row
	 * IDs are simply concatenated together, the number of bytes that need to be
	 * copied by the reducer is more than in a numerical summarization pattern.
	 * The same code for the reducer class is used as the combiner.
	 * 
	 * @author jack 2015年6月3日
	 *
	 */
	public static class Concatenator extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			StringBuilder sb = new StringBuilder();
			boolean first = true;
			for (Text id : values) {
				if (first) {
					first = false;
				} else {
					sb.append(" ");
				}
				sb.append(id.toString());
			}
			result.set(sb.toString());
			context.write(key, result);
		}

	}

	public static String getWikipediaURL(String text) {

		int idx = text.indexOf("\"http://en.wikipedia.org");
		if (idx == -1) {
			return null;
		}
		int idx_end = text.indexOf('"', idx + 1);

		if (idx_end == -1) {
			return null;
		}

		int idx_hash = text.indexOf('#', idx + 1);

		if (idx_hash != -1 && idx_hash < idx_end) {
			return text.substring(idx + 1, idx_hash);
		} else {
			return text.substring(idx + 1, idx_end);
		}

	}

	public static void main(String[] args) {
      
	}
}
