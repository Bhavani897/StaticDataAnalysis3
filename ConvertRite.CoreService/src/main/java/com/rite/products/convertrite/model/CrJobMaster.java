package com.rite.products.convertrite.model;

import lombok.Data;

import javax.persistence.*;
import java.util.Date;

@Entity
@Table(name = "CR_JOB_MASTER")
@Data
public class CrJobMaster {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "JOB_ID", nullable = false)
    private Long jobId;

    @Column(name = "SCHEDULE_ID", nullable = false)
    private Long scheduleId;

    @Column(name = "SOURCE_TEMPLATE_ID")
    private Long sourceTemplateId;

    @Column(name = "METADATA_TABLE_ID")
    private Long metadataTableId;

    @Column(name = "SOURCE_STAGING_TABLE_NAME", length = 255)
    private String sourceStagingTableName;

    @Column(name = "CLOUD_TEMPLATE_ID")
    private Long cloudTemplateId;

    @Column(name = "OBJECT_ID")
    private Long objectId;

    @Column(name = "BATCH_NAME", length = 255)
    private String batchName;

    @Column(name = "STATUS", length = 50)
    private String status;

    @Column(name = "LOAD_REQUEST_ID")
    private Long loadRequestId;

    @Column(name = "STANDARD_REQUEST_ID")
    private Long standardRequestId;

    @Column(name = "ERROR_MESSAGE", length = 2000)
    private String errorMessage;

    @Column(name = "JOB_START_TIME")
    private Date jobStartTime;

    @Column(name = "JOB_END_TIME")
    private Date jobEndTime;

    @Column(name = "CREATED_BY", length = 255)
    private String createdBy;

    @Column(name = "CREATED_DATE")
    private Date createdDate;

    @Column(name = "LAST_UPDATED_BY", length = 255)
    private String lastUpdatedBy;

    @Column(name = "LAST_UPDATED_DATE")
    private Date lastUpdatedDate;

    @Column(name = "FILE_PATH")
    private String filePath;
}