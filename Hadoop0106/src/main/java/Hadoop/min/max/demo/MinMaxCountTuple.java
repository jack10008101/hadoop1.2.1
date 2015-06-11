package Hadoop.min.max.demo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Writable;

/**
 * 该类存储三个属性最小最大和计数。作为mapper函数的输出。
 *  the minimum date, the maximum date, and the number of comments this user has created
 * 
 * @author jack 2015年5月28日
 *
 */
public class MinMaxCountTuple implements Writable {
	private Date min = new Date();
	private Date max = new Date();
	private long count = 0;
	private final static SimpleDateFormat frmt = new SimpleDateFormat(
			"yyyy-MM-dd'T'HH:mm:ss.SSS");

	public Date getMin() {
		return min;
	}

	public void setMin(Date min) {
		this.min = min;
	}

	public Date getMax() {
		return max;
	}

	public void setMax(Date max) {
		this.max = max;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		// Write the data out in the order it is read,
		// using the UNIX timestamp to represent the Date
		out.writeLong(min.getTime());
		out.writeLong(max.getTime());
		out.writeLong(count);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		min = new Date(in.readLong());
		max = new Date(in.readLong());
		count = in.readLong();
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return frmt.format(min) + "\t" + frmt.format(max) + "\t" + count;
	}
}
