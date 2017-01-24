package com.santosh.storm.test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class HostNameDetection {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		InetAddress ip = InetAddress.getByName("10.202.88.11");
		String hostname = ip.getHostName();
        String host=hostname.substring(0, hostname.indexOf(".")).toUpperCase();
        String tmpRegion=((hostname.substring(hostname.indexOf(".")+1)).substring(0, hostname.indexOf(".")).toUpperCase());
        String region=tmpRegion.substring(0,tmpRegion.indexOf("."));
        
        String line="10.203.37.5 - gsssanto [20/Dec/2016:02:30:12 -0800] \"GET /SupplierReconTool/userInfo.do HTTP/1.1\" 200 175";
		//System.out.println("***********************LINE::: "+line);
		String[] words=line.split(" ");
		System.out.println(words[3]+words[4]);
		String date=words[3]+words[4];
		date=date.substring(date.indexOf("[")+1,date.indexOf("]"));
		System.out.println(date);
		SimpleDateFormat standardFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ssZ");
		Date clientDate=standardFormat.parse(date);
        System.out.println(clientDate);
        System.out.println(standardFormat.format(clientDate));
	}

}
