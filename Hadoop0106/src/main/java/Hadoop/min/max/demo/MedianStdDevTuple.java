package Hadoop.min.max.demo;

public class MedianStdDevTuple {
	private float median;//中位数
	private float stdDev;//标准差

	public float getMedian() {
		return median;
	}

	public void setMedian(float median) {
		this.median = median;
	}

	public float getStdDev() {
		return stdDev;
	}

	public void setStdDev(float stdDev) {
		this.stdDev = stdDev;
	}
    @Override
    public String toString() {
    	// TODO Auto-generated method stub
    	return this.getMedian()+"\t"+this.getStdDev();
    }
}
