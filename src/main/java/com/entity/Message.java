package com.entity;

import java.io.Serializable;
import java.util.Date;

public class Message implements Serializable  {
	private static final long serialVersionUID = 5230225880231343722L;
	private  Long id;
	private  String name;
	private  String content;
	private  Date  createDate;
	
	
	public Message() {
		super();
	}
	
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}
	public Date getCreateDate() {
		return createDate;
	}
	public void setCreateDate(Date createDate) {
		this.createDate = createDate;
	}
	
	
	

}
