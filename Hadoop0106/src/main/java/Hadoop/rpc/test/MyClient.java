package Hadoop.rpc.test;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

/**
 * 
 * @author jack 2015年4月27日
 *
 */
public class MyClient {
	public static void main(String[] args) {
		InetSocketAddress inetSocketAddress = new InetSocketAddress(
				MyServer.IPADDRESS, MyServer.PORT);
		try {
			IProxyProtocol proxyProtocol = (IProxyProtocol) RPC.waitForProxy(IProxyProtocol.class,
					IProxyProtocol.VERSION, inetSocketAddress, new Configuration());
			int result=proxyProtocol.add(12, 5);
			System.out.println("the result is "+result);
			RPC.stopProxy(proxyProtocol);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
