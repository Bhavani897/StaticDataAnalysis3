package com.rite.products.convertrite.po;

import java.util.List;

import com.rite.products.convertrite.model.XxrCloudTable;

public class XxrCloudTemplatePo {

	private List<XxrCloudTable> cloudTableMetaData;
	private String[] templateHeaders;
	private String[] objectCodes;
	private String[] parentObjectCode;
	private String[] pod;
	private String[] bu;

	public String[] getPod() {
		return pod;
	}

	public void setPod(String[] pod) {
		this.pod = pod;
	}

	public String[] getBu() {
		return bu;
	}

	public void setBu(String[] bu) {
		this.bu = bu;
	}

	public String[] getObjectCodes() {
		return objectCodes;
	}

	public void setObjectCodes(String[] objectCodes) {
		this.objectCodes = objectCodes;
	}

	public String[] getParentObjectCode() {
		return parentObjectCode;
	}

	public void setParentObjectCode(String[] parentObjectCode) {
		this.parentObjectCode = parentObjectCode;
	}

	public List<XxrCloudTable> getCloudTableMetaData() {
		return cloudTableMetaData;
	}

	public void setCloudTableMetaData(List<XxrCloudTable> cloudTableMetaData) {
		this.cloudTableMetaData = cloudTableMetaData;
	}

	public String[] getTemplateHeaders() {
		return templateHeaders;
	}

	public void setTemplateHeaders(String[] templateHeaders) {
		this.templateHeaders = templateHeaders;
	}

}
