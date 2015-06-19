package Hadoop.dynamic.proxy.uni;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * JDK动态代理代理类,实现了InvocationHandler接口，invoke方法是调用具体的委托类中的方法
 * 
 * @author jack 2015年4月30日
 *
 */
public class BookFacadeProxy implements InvocationHandler {
	private Object target;

	/**
	 * 绑定委托对象并返回一个代理类
	 * 
	 * @param target
	 * @return
	 */
	public Object bind(Object target) {
		this.target = target;
		return Proxy.newProxyInstance(target.getClass().getClassLoader(),
				target.getClass().getInterfaces(), this);
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args)
			throws Throwable {
		// TODO Auto-generated method stub
        Object result=null;
        System.out.println("事物开始");
        result=method.invoke(target, args);
        System.out.println("事物结束");
		return result;
	}

}
