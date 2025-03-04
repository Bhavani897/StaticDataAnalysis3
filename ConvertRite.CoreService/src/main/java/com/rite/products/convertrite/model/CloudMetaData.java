package com.rite.products.convertrite.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name="XXR_LOOKUP_VAL_V")
public class CloudMetaData {
	@Id
	@Column(name="COLUMNID")
	private String columnId;
	
	@Column(name="VALUE")
	private String value;
	
	@Column(name="ID")
	private long id;
	
	@Column(name="PODID")
	private long podId;
	
	@Column(name="PROJECTID")
	private Long projectId;
	
	

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public long getPodId() {
		return podId;
	}

	public void setPodId(long podId) {
		this.podId = podId;
	}

	public Long getProjectId() {
		return projectId;
	}

	public void setProjectId(Long projectId) {
		this.projectId = projectId;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getColumnId() {
		return columnId;
	}

	public void setColumnId(String columnId) {
		this.columnId = columnId;
	}
	
	

}
