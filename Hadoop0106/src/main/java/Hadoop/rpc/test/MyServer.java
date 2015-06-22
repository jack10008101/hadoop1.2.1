package Hadoop.rpc.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;





public class MyServer {
	public static int PORT = 5432;
	public static String IPADDRESS = "127.0.0.1";

	public static void main(String[] args) {
     MyProxy proxy=new MyProxy();
    try {
    	//RPC.getServer(proxy, IPADDRESS, PORT, new Configuration()),第一个参数表示被调用的Java对象
		Server server=RPC.getServer(proxy, IPADDRESS, PORT, new Configuration());
		server.start();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	}
}
