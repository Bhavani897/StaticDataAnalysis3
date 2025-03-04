package com.rite.products.convertrite.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;

@Entity
@Table(name = "XXR_BLOB_CONVERRITE")
public class XxrBlobConvertrite {

	@Id
	@Column(name="ID")
	private String id;
	@Column(name = "BLOB_TYPE")
	private String blobType;
	@Column(name = "BLOB_NAME")
	private String blobName;
	@Lob
	@Column(name = "Blob")
	private String blob;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getBlobType() {
		return blobType;
	}

	public void setBlobType(String blobType) {
		this.blobType = blobType;
	}

	public String getBlobName() {
		return blobName;
	}

	public void setBlobName(String blobName) {
		this.blobName = blobName;
	}

	public String getBlob() {
		return blob;
	}

	public void setBlob(String blob) {
		this.blob = blob;
	}

}
