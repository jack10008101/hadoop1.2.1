package Hadoop.rpc.test;

import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.Text;

public class TextTest {

	public static void main(String[] args) throws UnsupportedEncodingException {
		// TODO Auto-generated method stub
		String s=new String("\u0041\u00df\uu6671\ud801\uDc00"); 
        Text t=new Text("\u0041\u00df\uu6671\ud801\uDc00"); 
        System.out.println(s.indexOf("\u0041"));
         System.out.println(t.find("\u0041"));
         System.out.println(s.indexOf("\u00df"));
       System.out.println(t.find("\u00df"));
       System.out.println(s.indexOf("\u6671"));
       System.out.println(t.find("\u6671"));
       System.out.println(s.indexOf("\ud801\uDc00"));
       System.out.println(t.find("\ud801\uDc00"));
       System.out.println(s.length());
       System.out.println(s.getBytes("UTF-8")); 
       System.out.println(t.getLength());
      
	}
}
