package Hadoop.dynamic.proxy.uni;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
/**
 * 实现调用处理器接口,实现InvocationHandler接口表示当前的类是对我们需要调用的类的代理类，在这个类
 * 里面包含了我们需要调用的类的引用
 * @author jack
 *  2015年4月29日
 *
 */
public class CalculatorHandler implements InvocationHandler{
    private Object objOriginal;
    public CalculatorHandler(Object object){
    	this.objOriginal=object;
    }
	@Override
	public Object invoke(Object proxy, Method method, Object[] args)
			throws Throwable {
		// TODO Auto-generated method stub
		//可添加一些预处理
		Object result=method.invoke(this.objOriginal, args);
		//可添加一些后续处理
		return result;
	}

}
