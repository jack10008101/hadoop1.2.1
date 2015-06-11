package Hadoop.dynamic.proxy.uni;
/**
 * 一个委托的类，需要被动态代理的类
 * @author jack
 *  2015年4月30日
 *
 */
public class BookFacadeImpl implements BookFacade {
    @Override
	public void addBook() {
		// TODO Auto-generated method stub
        System.out.println("增加图书方法。。。");
	}

}
