package com.rite.products.convertrite.model;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;

@Entity
@Table(name = "XXR_FILE_DATA_LOAD")
public class XxrFileDataLoad {
	@Id
	@Column(name = "TEMPLATE_NAME")
	private String templateName;
	@Column(name = "FILE_NAME")
	private String fileName;
	@Lob 
	@Column(name = "DATA_LOB")
	private String dataLob;
	@Column(name = "CREATION_DATE")
	private Date creationDate;

	public String getTemplateName() {
		return templateName;
	}

	public void setTemplateName(String templateName) {
		this.templateName = templateName;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getDataLob() {
		return dataLob;
	}

	public void setDataLob(String dataLob) {
		this.dataLob = dataLob;
	}

	public Date getCreationDate() {
		return creationDate;
	}

	public void setCreationDate(Date creationDate) {
		this.creationDate = creationDate;
	}

}
