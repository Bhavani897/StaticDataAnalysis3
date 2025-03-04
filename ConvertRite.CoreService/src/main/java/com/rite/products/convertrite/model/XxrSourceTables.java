package com.rite.products.convertrite.model;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name="XXR_SOURCE_TABLES")
public class XxrSourceTables implements Serializable {

	private static final long serialVersionUID = 1L;

	@EmbeddedId
	private SourceTableId sourceTableId;

	public SourceTableId getSourceTableId() {
		return sourceTableId;
	}

	public void setSourceTableId(SourceTableId sourceTableId) {
		this.sourceTableId = sourceTableId;
	}
	
	@Column(name="OBJECT_ID")
	private Long objectId;

	public Long getObjectId() {
		return objectId;
	}

	public void setObjectId(Long objectId) {
		this.objectId = objectId;
	}

	
	
	
	

}
