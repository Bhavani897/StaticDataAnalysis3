package com.rite.products.convertrite.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

import javax.persistence.*;
import java.sql.Date;
@Data
@Entity
@Table(name = "CR_VALIDATE_CVR_CCID")
@IdClass(CrValidateCvrCcidId.class)
public class CrValidateCvrCcid {
    @Column(name = "CLOUD_STAGING_TABLE_NAME")
    private String cloudStagingTableName;
    @Id
    @Column(name = "CR_BATCH_NAME")
    private String crBatchName;
    @Id
    @Column(name = "CCID")
    private String ccid;
    @Column(name = "ERROR_MESSAGE")
    private String errorMessage;
    @Column(name = "ERROR_CODE")
    private String errorCode;
    @Column(name = "STATUS")
    private String status;
    @Column(name = "CREATION_DATE")
    private Date creationDate;
    @Column(name = "LAST_UPDATE_DATE")
    private Date lastUpdatedDate;
    @Column(name = "CREATED_BY")
    private String createdBy;
    @Column(name = "LAST_UPDATED_BY")
    private String lastUpdateBy;
    @Id
    @Column(name="REQUEST_ID")
    private Long requestId;
}
