package Hadoop.dynamic.proxy.uni;



public class TestProxy {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
        BookFacadeProxy proxy=new BookFacadeProxy();
       BookFacade proxyBookFacade=(BookFacade) proxy.bind(new BookFacadeImpl());
       proxyBookFacade.addBook();
	}

}
