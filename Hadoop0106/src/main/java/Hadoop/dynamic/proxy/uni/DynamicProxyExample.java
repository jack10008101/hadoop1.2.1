package Hadoop.dynamic.proxy.uni;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;

public class DynamicProxyExample {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		CalculatorProtocol server = new Server();
		InvocationHandler invocationHandler = new CalculatorHandler(server);
		CalculatorProtocol client=(CalculatorProtocol) Proxy.newProxyInstance(server.getClass().getClassLoader(), server
				.getClass().getInterfaces(), invocationHandler);
		int result=client.add(1, 2);
		System.out.println(result);
	}
}
