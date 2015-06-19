package Hadoop.rpc.test;

import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * hadoop中所有的自定义的RPC接口都需要集成VersionedProtocol接口,他描述了协议的版本信息
 * 
 * @author jack 2015年4月27日
 *
 */
public interface IProxyProtocol extends VersionedProtocol {
	public static final long VERSION = 23234L;// 版本号，默认的情况下不同的版本号的RPC
												// client和server之间不能通信，客户端和服务端通过版本号进行识别
	/**
	 * 声明一个add方法，两个整数的相加
	 * @param num1
	 * @param num2
	 * @return
	 */
	int add(int num1,int num2);

}
