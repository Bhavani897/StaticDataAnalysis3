package com.rite.products.convertrite.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Header {
	@JsonProperty("To")
	private String to;
	@JsonProperty("Action")
	private String action;
	@JsonProperty("MessageID")
	private String messageID;
	@JsonProperty("RelatesTo")
	private Long relatesTo;
	

	public Long getRelatesTo() {
		return relatesTo;
	}

	public void setRelatesTo(Long relatesTo) {
		this.relatesTo = relatesTo;
	}

	public String getTo() {
		return to;
	}

	public void setTo(String to) {
		this.to = to;
	}

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}

	public String getMessageID() {
		return messageID;
	}

	public void setMessageID(String messageID) {
		this.messageID = messageID;
	}

}
