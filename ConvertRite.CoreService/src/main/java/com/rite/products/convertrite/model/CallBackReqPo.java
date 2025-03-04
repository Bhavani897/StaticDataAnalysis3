package com.rite.products.convertrite.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CallBackReqPo {

	private Header header;
	
	private Body body;
	@JsonProperty("Header")
	public Header getHeader() {
		return header;
	}

	public void setHeader(Header header) {
		this.header = header;
	}

	@JsonProperty("Body")
	public Body getBody() {
		return body;
	}

	public void setBody(Body body) {
		this.body = body;
	}
	
	



	

}
