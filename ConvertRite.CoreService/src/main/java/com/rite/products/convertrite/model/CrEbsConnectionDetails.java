package com.rite.products.convertrite.model;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;


import lombok.Data;

@Entity
@Table(name = "CR_EBS_CONNECTION_DETAILS")
@Data
public class CrEbsConnectionDetails {
	@Id
	@Column(name = "CONNECT_ID")
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long connectionId;
	@Column(name = "CONNECTION_NAME")
	private String connectionName;
	@Column(name = "HOST_NAME")
	private String hostName;
	@Column(name = "SERVICE_NAME")
	private String serviceName;
	@Column(name = "USER_NAME")
	private String userName;
	@Column(name = "PASSWORD")
	private String password;
	@Column(name = "PORT")
	private Integer port;
	@Column(name = "DATABASE_LINK")
	private String dabaseLink;
	@Column(name = "CREATION_DATE")
	private Date creationDate;
	@Column(name = "LAST_UPDATE_DATE")
	private Date lastUpdatedDate;
	@Column(name = "CREATED_BY")
	private String createdBy;
	@Column(name = "LAST_UPDATED_BY")
	private String lastUpdateBy;
	@Column(name = "CONNECTION_TYPE")
	private String connectionType;
}
