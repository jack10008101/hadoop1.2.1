package Hadoop.map.reduce.average.count;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CountAverageTuple implements Writable {
	private float count;
	private float average;

	public float getCount() {
		return count;
	}

	public void setCount(float count) {
		this.count = count;
	}

	public float getAverage() {
		return average;
	}

	public void setAverage(float average) {
		this.average = average;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return count + "\t" + average;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeFloat(count);
		out.writeFloat(average);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		count = in.readFloat();
		average = in.readFloat();
	}
}
