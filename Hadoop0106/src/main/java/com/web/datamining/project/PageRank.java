package com.web.datamining.project;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;


/**
有1，2，3，4四个网页，计算他们的pagerank的值， 得到的矩阵关系如下：
0.0,1/2,0.0,0.0,
1/3,0.0,0.0,1/2,
1/3,0.0,1.0,1/2,
1/3,1/2,0.0,0.0,
 * ，通过不断的迭代，当q(next
 * )和q(current)之间的距离小于0.0000001时，认为已经收敛。pagerank就是特征值为1的特征向量
 * ，1,2,3,4号网页的价值分别为特征向量中对应维的值
 * 
 * @author jack 2015年6月19日
 *
 */
public class PageRank {
	private static final double ALPHA = 0.8;
	private static final double DISTANCE = 0.0000001;

	public static void main(String[] args) {
		System.out.println("alpha的值为：" + ALPHA);
		List<Double> q1 = new ArrayList<Double>();
		q1.add(new Double(1/4));
		q1.add(new Double(1/4));
		q1.add(new Double(1/4));
		q1.add(new Double(1/4));
		System.out.println("初始化的向量Q为：");
		printVector(q1);
		System.out.println("初始化的转移矩阵S为：");
		//printMatrix(getG(ALPHA));
		printMatrix(getS());
		List<Double> pageRank = calPageRank(q1, ALPHA);
		System.out.println("PageRank为：");
		printVector(pageRank);
	}

	private static List<Double> calPageRank(List<Double> q1, double alpha2) {
		// TODO Auto-generated method stub
		/*
		 * v'=alpha*S*V+(1-alpha)U/n,其中n表示节点数V是一个n*1的列向量，S是一个n*n的矩阵 U是一个n*1的列向量
		 */
		List<Double> q = null;
		List<Double> secondPart = calculateVectorMultipleNum(getU(), alpha2,
				q1.size());
		while (true) {
			List<Double> firstPart = calculateMatrixMultiple(getS(), q1,
					alpha2);
			q = addVector(firstPart, secondPart);
			double distance = calculateDistance(q, q1);
			System.out.println("向量q和向量q1之间的distance：" + distance);
			if (distance <= DISTANCE) {
				System.out.println("向量q1：");
				printVector(q1);
				System.out.println("向量q：");
				printVector(q);
				break;
			}
			q1 = q;
		}
		return q;
	}
    /**
     * 计算两个向量之间的距离
     * @param q 第一个向量
     * @param q1 第二个向量
     * @return 两个向量之间的距离
     */
	private static double calculateDistance(List<Double> q, List<Double> q1) {
		// TODO Auto-generated method stub
		double sum=0.0;
		if (q.size()!=q1.size()) {
			return -1;
		}
		for (int i = 0; i < q1.size(); i++) {
			sum+=Math.pow(q1.get(i).doubleValue()-q.get(i).doubleValue(), 2);
		}
		return Math.sqrt(sum);
	}
    /**
     * 将两个N*1维的列向量相加，因为两个列向量都是N*1维，所以使用一个List<Double>来存储一个列向量
     * @param firstPart
     * @param secondPart
     * @return
     */
	private static List<Double> addVector(List<Double> firstPart,
			List<Double> secondPart) {
		// TODO Auto-generated method stub
		if (firstPart==null||secondPart==null||firstPart.size()==0) {
			return null;
		}
		List<Double> result=new ArrayList<Double>();
		for (int i = 0; i < firstPart.size(); i++) {
			result.add(firstPart.get(i)+secondPart.get(i));
		}
		return result;
	}

	/**
	 * 计算一个N*1的列向量乘以一个系数（1-alpha）/size
	 * 
	 * @param u
	 *            N*1的列向量
	 * @param alpha2
	 * @param size
	 *            向量的维数
	 * @return 返回一个N*1的列向量
	 */
	private static List<Double> calculateVectorMultipleNum(
			List<List<Double>> u, double alpha2, int size) {
		// TODO Auto-generated method stub
		List<Double> result = new ArrayList<Double>();
		double temp = 0.0;
		for (int i = 0; i < u.size(); i++) {
			for (int j = 0; j < 1; j++) {
				temp = u.get(i).get(j).doubleValue() * (1 - alpha2) / size;
			}
			result.add(temp);
		}

		return result;
	}

	/**
	 * 计算一个矩阵乘以一个向量，最后的结果乘以一个alpha，返回一个向量
	 * 
	 * @param s
	 *            一个矩阵
	 * @param q1
	 *            一个向量
	 * @param alpha2
	 *            一个系数
	 * @return 返回一个向量N*1
	 */
	private static List<Double> calculateMatrixMultiple(List<List<Double>> s,
			List<Double> q1, double alpha2) {
		// TODO Auto-generated method stub
		if (s == null || q1 == null || s.size() == 0
				|| (s.get(0).size() != q1.size())) {
			return null;
		}
		List<Double> result = new ArrayList<Double>();
		for (int i = 0; i < s.size(); i++) {
			double sum = 0.0;
			for (int j = 0; j < s.get(i).size(); j++) {
				sum += s.get(i).get(j).doubleValue() * q1.get(j).doubleValue();
			}
			result.add(sum * alpha2);
		}
		return result;
	}

	/**
	 * 打印输出一个矩阵
	 * 
	 * @param g
	 */
	private static void printMatrix(List<List<Double>> g) {
		// TODO Auto-generated method stub
		for (int i = 0; i < g.size(); i++) {
			for (int j = 0; j < g.get(i).size(); j++) {
				System.out.print(g.get(i).get(j) + ",");
			}
			System.out.println();
		}
	}

	/**
	 * 计算获得初始的G矩阵
	 * 
	 * @param alpha2
	 *            =0.85
	 * @return 初始矩阵
	 */
	private static List<List<Double>> getG(double alpha2) {
		// TODO Auto-generated method stub
		int num = getS().size();
		List<List<Double>> aS = numberMulMatrix(getS(), alpha2);
		return null;
	}

	private static List<List<Double>> numberMulMatrix(List<List<Double>> s,
			double alpha2) {
		// TODO Auto-generated method stub

		return null;
	}

	/**
	 * 返回初始化转移矩阵S
	 * 
	 * @return
	 */
	private static List<List<Double>> getS() {
		// TODO Auto-generated method stub
		List<Double> row1 = new ArrayList<Double>();
		row1.add(new Double(0));
		row1.add(new Double(1/2.0));
		row1.add(new Double(0));
		row1.add(new Double(0));
		List<Double> row2 = new ArrayList<Double>();
		row2.add(new Double(1 / 3.0));
		row2.add(new Double(0));
		row2.add(new Double(0));
		row2.add(new Double(1/2.0));
		List<Double> row3 = new ArrayList<Double>();
		row3.add(new Double(1 / 3.0));
		row3.add(new Double(0));
		row3.add(new Double(1));
		row3.add(new Double(1/2.0));
		List<Double> row4 = new ArrayList<Double>();
		row4.add(new Double(1 / 3.0));
		row4.add(new Double(1 / 2.0));
		row4.add(new Double(0));
		row4.add(new Double(0));
		List<List<Double>> result = new ArrayList<List<Double>>();
		result.add(row1);
		result.add(row2);
		result.add(row3);
		result.add(row4);
		return result;
	}

	/**
	 * 初始化矩阵U，全为1，v'=alpha*S*V+(1-alpha)U/n,其中n表示节点数V是一个n*1的列向量，S是一个n*n的矩阵
	 * U是一个n*1的列向量
	 * 
	 * @return
	 */
	public static List<List<Double>> getU() {
		List<Double> row1 = new ArrayList<Double>();
		row1.add(new Double(1));
		List<Double> row2 = new ArrayList<Double>();
		row2.add(new Double(1));
		List<Double> row3 = new ArrayList<Double>();
		row3.add(new Double(1));
		List<Double> row4 = new ArrayList<Double>();
		row4.add(new Double(1));
		List<List<Double>> result = new ArrayList<List<Double>>();
		result.add(row1);
		result.add(row2);
		result.add(row3);
		result.add(row4);
		return result;
	}

	/**
	 * 打印输出一个向量
	 * 
	 * @param q1
	 */
	private static void printVector(List<Double> q1) {
		// TODO Auto-generated method stub
		for (int i = 0; i < q1.size(); i++) {
			System.out.print(q1.get(i) + ",");
		}
		System.out.println();
	}

	/**
	 * 获得一个随机的初始向量Q
	 * 
	 * @param n
	 *            向量的维数
	 * @return 一个随机向量，每一维的数字大小在0-5之间的随机数
	 */
	private static List<Double> getInitQ(int n) {
		Random random = new Random();
		List<Double> qDoubles = new ArrayList<Double>();
		for (int i = 0; i < n; i++) {
			qDoubles.add(new Double(random.nextDouble() * 5));
		}
		return qDoubles;

	}
}
/**结果：
alpha的值为：0.8
初始化的向量Q为：
0.0,0.0,0.0,0.0,
初始化的转移矩阵S为：
0.0,0.5,0.0,0.0,
0.3333333333333333,0.0,0.0,0.5,
0.3333333333333333,0.0,1.0,0.5,
0.3333333333333333,0.5,0.0,0.0,
向量q和向量q1之间的distance：0.09999999999999998
向量q和向量q1之间的distance：0.08944271909999157
向量q和向量q1之间的distance：0.08279559838005345
向量q和向量q1之间的distance：0.07491007942860557
向量q和向量q1之间的distance：0.06548958850435034
向量q和向量q1之间的distance：0.05581419499727123
向量q和向量q1之间的distance：0.046706160731050626
向量q和向量q1之间的distance：0.03858536406968528
向量q和向量q1之间的distance：0.03158832975332764
向量q和向量q1之间的distance：0.02569382761917806
向量q和向量q1之间的distance：0.020803123494426954
向量q和向量q1之间的distance：0.016787672325648713
向量q和向量q1之间的distance：0.013515001649241711
向量q和向量q1之间的distance：0.010861571040939053
向量q和向量q1之间的distance：0.008718194993268116
向量q和向量q1之间的distance：0.006991443110040867
向量q和向量q1之间的distance：0.005603006356646057
向量q和向量q1之间的distance：0.004488151467994561
向量q和向量q1之间的distance：0.00359387243026389
向量q和向量q1之间的distance：0.0028770521718476045
向量q和向量q1之间的distance：0.002302781225477277
向量q和向量q1之间的distance：0.0018428893663325944
向量q和向量q1之间的distance：0.001474698852150404
向量q和向量q1之间的distance：0.0011799849183651047
向量q和向量q1之间的distance：9.441195983403085E-4
向量q和向量q1之间的distance：7.553724379121763E-4
向量q和向量q1之间的distance：6.043427000756631E-4
向量q和向量q1之间的distance：4.835002484295227E-4
向量q和向量q1之间的distance：3.8681540774518114E-4
向量q和向量q1之间的distance：3.094611927038052E-4
向量q和向量q1之间的distance：2.475741231223656E-4
向量q和向量q1之间的distance：1.9806231186893708E-4
向量q和向量q1之间的distance：1.5845160621061078E-4
向量q和向量q1之间的distance：1.2676230908593365E-4
向量q和向量q1之间的distance：1.0141044430057725E-4
向量q和向量q1之间的distance：8.112870349326236E-5
向量q和向量q1之间的distance：6.490316569952121E-5
向量q和向量q1之间的distance：5.1922650847413394E-5
向量q和向量q1之间的distance：4.1538189636289176E-5
向量q和向量q1之间的distance：3.323059190979873E-5
向量q和向量q1之间的distance：2.6584496963603087E-5
向量q和向量q1之间的distance：2.1267611233233025E-5
向量q和向量q1之间的distance：1.7014096951544374E-5
向量q和向量q1之间的distance：1.3611282204452915E-5
向量q和向量q1之间的distance：1.0889028470462968E-5
向量q和向量q1之间的distance：8.711224354307844E-6
向量q和向量q1之间的distance：6.968980403421349E-6
向量q和向量q1之间的distance：5.575184859107971E-6
向量q和向量q1之间的distance：4.460148199880747E-6
向量q和向量q1之间的distance：3.568118742247617E-6
向量q和向量q1之间的distance：2.8544951000686376E-6
向量q和向量q1之间的distance：2.283596141960944E-6
向量q和向量q1之间的distance：1.826876949717616E-6
向量q和向量q1之间的distance：1.4615015806462854E-6
向量q和向量q1之间的distance：1.1692012769071172E-6
向量q和向量q1之间的distance：9.35361028631121E-7
向量q和向量q1之间的distance：7.482888271237442E-7
向量q和向量q1之间的distance：5.986310640526682E-7
向量q和向量q1之间的distance：4.7890485266322E-7
向量q和向量q1之间的distance：3.8312388306316336E-7
向量q和向量q1之间的distance：3.0649910698343774E-7
向量q和向量q1之间的distance：2.451992857643859E-7
向量q和向量q1之间的distance：1.9615942870032654E-7
向量q和向量q1之间的distance：1.5692754318230584E-7
向量q和向量q1之间的distance：1.2554203454584467E-7
向量q和向量q1之间的distance：1.0043362763667574E-7
向量q和向量q1之间的distance：8.034690224256735E-8
向量q1：
0.10135135135135132,0.12837837837837832,0.6418914901573809,0.12837837837837832,
向量q：
0.10135135135135132,0.12837837837837834,0.6418915705042831,0.12837837837837834,
PageRank为：
0.10135135135135132,0.12837837837837834,0.6418915705042831,0.12837837837837834,
 */