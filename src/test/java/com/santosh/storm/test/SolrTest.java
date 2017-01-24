package com.santosh.storm.test;

import java.io.IOException;

import org.apache.solr.client.solrj.SolrQuery;

import org.apache.solr.client.solrj.impl.HttpSolrClient;

import com.santosh.storm.spout.WebAccessLogSpout;

public class SolrTest {

	public static HttpSolrClient solrServer;
	public static String solrUrl="http://localhost:8983/solr/";
	public static String solrCollection="mostactive";
	
	
	public static void main(String[] args) throws Exception, IOException {
		
	String url="/SupplierReconTool/userInfo.do";
	if(url.contains("?"))
	url=url.substring(0,url.indexOf("?"));
	System.out.println(url);
	}

}
