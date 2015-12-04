package com.fdms.batch.data.migration.vo;

public class DMScript {
	
	private String script;
	private String queryType;
    private int orderNoGroup;
    private int scriptId;
    private int orderNo;
       
	public String getScript() {
		return script;
	}
	public void setScript(String script) {
		this.script = script;
	}
	public String getQueryType() {
		return queryType;
	}
	public void setQueryType(String queryType) {
		this.queryType = queryType;
	}
	
	public int getOrderNo() {
		return orderNo;
	}
	public void setOrderNo(int orderNo) {
		this.orderNo = orderNo;
	}
	public int getScriptId() {
		return scriptId;
	}
	public void setScriptId(int scriptId) {
		this.scriptId = scriptId;
	}
	public int getOrderNoGroup() {
		return orderNoGroup;
	}
	public void setOrderNoGroup(int orderNoGroup) {
		this.orderNoGroup = orderNoGroup;
	}
    
}
