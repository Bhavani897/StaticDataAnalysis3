package com.rite.products.convertrite.po;

import com.rite.products.convertrite.model.XxrTemplateRelation;

public class ZipCsvFilesResPo {

	private XxrTemplateRelation xxrTemplateRelation;

	private String zipFilePath;

	public XxrTemplateRelation getXxrTemplateRelation() {
		return xxrTemplateRelation;
	}

	public void setXxrTemplateRelation(XxrTemplateRelation xxrTemplateRelation) {
		this.xxrTemplateRelation = xxrTemplateRelation;
	}

	public String getZipFilePath() {
		return zipFilePath;
	}

	public void setZipFilePath(String zipFilePath) {
		this.zipFilePath = zipFilePath;
	}

}
