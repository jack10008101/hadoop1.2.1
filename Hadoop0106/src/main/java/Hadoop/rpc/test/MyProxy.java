package Hadoop.rpc.test;

import java.io.IOException;

/**
 * hadoop RPC协议是一个Java接口，我们自定义自己的MyProxy类去实现IProxyProtocol接口
 * @author jack
 *  2015年4月27日
 *
 */
public class MyProxy implements IProxyProtocol{

	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		// TODO Auto-generated method stub
		System.out.println("MyProxy.ProtocolVersion is "+IProxyProtocol.VERSION);
		
		return IProxyProtocol.VERSION;
	}

	public int add(int num1, int num2) {
		// TODO Auto-generated method stub
		System.out.println("我被调用了，add方法");
		return num1+num2;
	}

}
