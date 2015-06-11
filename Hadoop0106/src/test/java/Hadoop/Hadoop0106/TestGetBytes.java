package Hadoop.Hadoop0106;

public class TestGetBytes {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String teString = "1,三劫散仙,13575468248";
		byte[] bytes = teString.getBytes();
		for (byte b : bytes) {
			System.out.println(b);
		}
	}

}
