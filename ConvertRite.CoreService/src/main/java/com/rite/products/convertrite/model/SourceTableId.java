package com.rite.products.convertrite.model;

import lombok.Data;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Embeddable;
@Data
@Embeddable
public class SourceTableId implements Serializable{
	
	private static final long serialVersionUID = 1L;
	
	@Column(name = "TABLE_ID")
	private long tableId;

	@Column(name = "TABLE_NAME")
	private String tableName;

}
