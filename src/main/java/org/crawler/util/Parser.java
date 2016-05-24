package org.crawler.util;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 工具类Parser.java
 * 分析网页,提取URL
 * @author lin
 *
 */
public class Parser {

//	private static Logger logger = LoggerFactory.getLogger(Parser.class);
//	private static int count = 1;
	
	public static Set<String> parser(String html){

		Set<String> urls = new HashSet<String>();
		String regex = "<a.*?/a>";
		Pattern pt = Pattern.compile(regex);
		Matcher matcher = pt.matcher(html);
		while(matcher.find()){
			//获取网址
//			System.out.println("Group:"+matcher.group());
			Matcher myurl = Pattern.compile("href=\".*?\">").matcher(matcher.group());
			if(myurl.find()){
				do{
					String temp = myurl.group();
					if(!temp.startsWith("javascript") &&  !temp.startsWith("/") && ! temp.startsWith("#")){
						String  url = myurl.group().replaceAll("href=\"|\">","");
						if(url.contains("http")){
							if(url.contains("\"")){
								url = url.substring(0, url.indexOf("\""));
							}
							if(!url.contains("$") && !url.contains(" ")){
								urls.add(url);
							}
						}
					}
				}while(myurl.find());
			}
		}
		return urls;
	}
}
