package com.rite.products.convertrite.model;

import java.sql.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

@Data
@Entity
@Table(name = "`xxr_source_load_fail_records`")
//@IdClass(XxrSourceLoadFailRecordsId.class)
public class XxrSourceLoadFailRecords {
	
	@Column(name="TEMPLATE_ID")
	private Long templateId;
	@Id
	@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "src_load_fail_rec_gen")
	@SequenceGenerator(name="src_load_fail_rec_gen", sequenceName = "xxr_source_load_fail_s",allocationSize=1)
	@Column(name="ID")
	private Long id;
	@Column(name = "FILE_NAME")
	private String fileName;
	@Lob
	@Column(name = "FAILED_CLOB")
	private String failedClob;
	@Column(name = "SUCCESS")
	private long success;
	@Column(name = "FAILED")
	private long failed;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	@Column(name = "CREATION_DATE")
	private Date creationDate;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
	@Column(name = "LAST_UPDATED_DATE")
	private Date lastUpdatedDate;
	@Column(name = "CREATED_BY")
	private String createdBy;
	@Column(name = "LAST_UPDATED_BY")
	private String lastUpdateBy;
	@Lob
	@Column(name="LOG_FILE_BLOB")
	private byte[] logFileBlob;
	@Column(name="XXR_BATCH_NAME")
	private String xxrBatchName;

}
