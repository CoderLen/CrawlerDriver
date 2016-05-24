package org.crawler.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.hadoop.io.Text;

public class Downloader{
	public static String Download(Text pageURL){
		  StringBuilder pageHTML = new StringBuilder(); 
		  BufferedReader br = null;
	        try { 
	            URL url = new URL(pageURL.toString()); 
	            HttpURLConnection connection = (HttpURLConnection) url.openConnection(); 
	            connection.setRequestProperty("User-Agent", "MSIE 7.0"); 
	            br = new BufferedReader(new InputStreamReader(connection.getInputStream(), "utf-8")); 
	            String line = null; 
	            while ((line = br.readLine()) != null) { 
	                pageHTML.append(line);
	            } 
	            connection.disconnect(); 
	        } catch (Exception e) { 
	            e.printStackTrace(); 
	        } finally{
	        	try {
					br.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	        }
	        return pageHTML.toString();
	}
	
	public static void main(String[] a){
		String htm = Downloader.Download(new Text("http://bbs.csdn.net/topics/380101431"));
		System.out.println("RS:"+htm);
	}
}