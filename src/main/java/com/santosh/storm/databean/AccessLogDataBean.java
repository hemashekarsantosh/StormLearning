package com.santosh.storm.databean;

import java.io.Serializable;

public class AccessLogDataBean implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String appName;
	private String remoteHost;
	private String date;
	private String clientformatDate;
	private String authuser;
	private String requesturl;
	private String requestStatus;
	private String requestMethod;
	private String completeMessage;
	
	
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public String getClientformatDate() {
		return clientformatDate;
	}
	public void setClientformatDate(String clientformatDate) {
		this.clientformatDate = clientformatDate;
	}
	public String getAppName() {
		return appName;
	}
	public void setAppName(String appName) {
		this.appName = appName;
	}
	public String getRemoteHost() {
		return remoteHost;
	}
	public void setRemoteHost(String remoteHost) {
		this.remoteHost = remoteHost;
	}
	public String getAuthuser() {
		return authuser;
	}
	public void setAuthuser(String authuser) {
		this.authuser = authuser;
	}
	public String getRequesturl() {
		return requesturl;
	}
	public void setRequesturl(String requesturl) {
		this.requesturl = requesturl;
	}
	public String getRequestStatus() {
		return requestStatus;
	}
	public void setRequestStatus(String requestStatus) {
		this.requestStatus = requestStatus;
	}
	public String getRequestMethod() {
		return requestMethod;
	}
	public void setRequestMethod(String requestMethod) {
		this.requestMethod = requestMethod;
	}
	public String getCompleteMessage() {
		return completeMessage;
	}
	public void setCompleteMessage(String completeMessage) {
		this.completeMessage = completeMessage;
	}
	
		
}
