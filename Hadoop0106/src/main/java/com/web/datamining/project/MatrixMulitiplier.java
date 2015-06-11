package com.web.datamining.project;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MatrixMulitiplier {
	public static class Job1Key implements WritableComparable<Job1Key> {
		private int matrixSeq;
		private int i;

		public int getMatrixSeq() {
			return matrixSeq;
		}

		public void setMatrixSeq(int matrixSeq) {
			this.matrixSeq = matrixSeq;
		}

		public int getI() {
			return i;
		}

		public void setI(int i) {
			this.i = i;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeInt(matrixSeq);
			out.writeInt(i);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			this.matrixSeq = in.readInt();
			this.i = in.readInt();
		}

		@Override
		public int compareTo(Job1Key o) {
			// TODO Auto-generated method stub
			if (this.i < o.i) {
				return -1;
			} else if (this.i > o.i) {
				return 1;
			} else {
				if (this.matrixSeq < o.matrixSeq) {
					return -1;
				} else if (this.matrixSeq > o.matrixSeq) {
					return 1;
				} else {
					return 0;
				}
			}

		}

		@Override
		public String toString() {
			// TODO Auto-generated method stub
			return matrixSeq + "," + i;
		}
	}

	public static class Job1Value implements WritableComparable<Job1Value>,
			Cloneable {
		private int i;
		private int j;
		private int value;

		public int getI() {
			return i;
		}

		public void setI(int i) {
			this.i = i;
		}

		public int getJ() {
			return j;
		}

		public void setJ(int j) {
			this.j = j;
		}

		public int getValue() {
			return value;
		}

		public void setValue(int value) {
			this.value = value;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeInt(i);
			out.writeInt(j);
			out.writeInt(value);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			this.i = in.readInt();
			this.j = in.readInt();
			this.value = in.readInt();
		}

		@Override
		public int compareTo(Job1Value o) {
			// TODO Auto-generated method stub
			if (this.i < o.i) {
				return -1;
			} else if (this.i > o.i) {
				return 1;
			}
			return 0;
		}

		@Override
		public String toString() {
			return i + "," + j + "," + value;
		}

		@Override
		public Job1Value clone() throws CloneNotSupportedException {
			return (Job1Value) super.clone();
		}
	}

	public static class Job1Partitioner extends Partitioner<Job1Key, Job1Value> {

		@Override
		public int getPartition(Job1Key key, Job1Value value, int numPartitions) {
			// TODO Auto-generated method stub
			return key.getI() % numPartitions;
		}

	}

	public static class Job1Mapper extends
			Mapper<IntWritable, Text, Job1Key, Job1Value> {
		private boolean isLeft=false;
		private Job1Key outKey=new Job1Key();
		private Job1Value outValue=new Job1Value();
        @Override
        protected void setup(
        		Mapper<IntWritable, Text, Job1Key, Job1Value>.Context context)
        		throws IOException, InterruptedException {
        	// TODO Auto-generated method stub
        	FileSplit split=(FileSplit) context.getInputSplit();
        	isLeft=split.getPath().toString().indexOf("a.txt")!=-1;
        }
		@Override
		protected void map(IntWritable key, Text value,
				Mapper<IntWritable, Text, Job1Key, Job1Value>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
            
		}
	}
}
