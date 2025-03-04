package com.rite.products.convertrite.model;

import lombok.Data;

import javax.persistence.*;
import java.util.Date;

@Entity
@Data
@Table(name="CR_CLOUD_JOB_STATUS")
public class CrCloudJobStatus {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "JOB_ID")
    private long jobId;
    @Column(name = "OBJECT_CODE")
    private String objectCode;
    @Column(name = "OBJECT_ID")
    private long objectId;
    @Column(name = "CLD_TEMPLATE_ID")
    private long cldTemplateId;
    @Column(name = "TYPE")
    private String importType;
    @Column(name = "DOCUMENT_AUTHOR")
    private String documentAuthor;
    @Column(name = "DOCUMENT_TITLE")
    private String documentTitle;
    @Column(name = "DOCUMENT_SECURITY_GROUP")
    private String documentSecurityGroup;
    @Column(name = "DOCUMENT_ACCOUNT")
    private String documentAccount;
    @Column(name = "CONTENT_ID")
    private String contentId;
    @Column(name = "JOB_NAME")
    private String jobName;
    @Column(name = "PARAMETER_LIST")
    private String parameterList;
    @Column(name = "INTERFACE_ID")
    private String interfaceId;
    @Column(name = "LOAD_REQUEST_ID")
    private long loadRequestId;
    @Column(name = "JOB_STATUS")
    private String jobStatus;
    @Column(name = "BATCH_NAME")
    private String batchName;
    @Column(name = "ATTRIBUTE1")
    private String attribute1;
    @Column(name = "ATTRIBUTE2")
    private String attribute2;
    @Column(name = "ATTRIBUTE3")
    private String attribute3;
    @Column(name = "ATTRIBUTE4")
    private String attribute4;
    @Column(name = "ATTRIBUTE5")
    private String attribute5;
    @Column(name = "LAST_UPDATE_DATE")
    private Date lastUpdatedDate;
    @Column(name = "LAST_UPDATED_BY")
    private String lastUpdatedBy;
    @Column(name = "CREATION_DATE")
    private Date creationDate;
    @Column(name = "CREATED_BY")
    private String createdBy;
    @Column(name = "JOB_ERROR_MESSAGE")
    private String errorMsg;
    @Column(name = "ADDITIONAL_INFO")
    private String additionalInfo;
}
