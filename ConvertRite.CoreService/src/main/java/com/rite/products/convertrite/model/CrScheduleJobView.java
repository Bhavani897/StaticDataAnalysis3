package com.rite.products.convertrite.model;

import java.time.LocalDateTime;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;
import lombok.Data;

@Entity
@Table(name = "cr_view_schedule_job_details")
@Data
@IdClass(CrScheduleJobViewId.class)
public class CrScheduleJobView {

    @Column(name = "object_name")
    private String objectName;

    @Column(name = "cld_template_name")
    private String cldTemplateName;

//    @Column(name = "template_id")
//    private Long templateId;

    @Column(name = "parent_object_name")
    private String parentObjectName;
    @Id
    @Column(name = "SCHEDULE_ID")
    private Long scheduleId;

    @Column(name = "IS_RELATIONAL")
    private Boolean isRelational;

    @Column(name = "SCHEDULE_NAME")
    private String scheduleName;
    @Id
    @Column(name = "BATCH_NAME")
    private String batchName;

    @Column(name = "SCHEDULE_FREQUENCY")
    private String scheduleFrequency;

    @Column(name = "SOURCE_SYSTEM_ID")
    private Long sourceSystemId;

    @Column(name = "TARGET_SYSTEM_ID")
    private Long targetSystemId;

    @Column(name = "EXTRACTION_TYPE")
    private String extractionType;

    @Column(name = "SCHEDULE_START_TIME")
    private LocalDateTime scheduleStartTime;

    @Column(name = "SCHEDULE_LAST_END_TIME")
    private LocalDateTime scheduleLastEndTime;

    @Column(name = "STATUS")
    private String status;

    @Column(name = "CREATED_BY")
    private String createdBy;

    @Column(name = "CREATED_DATE")
    private LocalDateTime createdDate;

    @Column(name = "LAST_UPDATED_BY")
    private String lastUpdatedBy;

    @Column(name = "LAST_UPDATED_DATE")
    private LocalDateTime lastUpdatedDate;

    @Column(name = "JOB_ID")
    private Long jobId;
    @Id
    @Column(name = "SOURCE_TEMPLATE_ID")
    private Long sourceTemplateId;

    @Column(name = "METADATA_TABLE_ID")
    private Long metadataTableId;

    @Column(name = "SOURCE_STAGING_TABLE_NAME")
    private String sourceStagingTableName;

    @Column(name = "CLOUD_TEMPLATE_ID")
    private Long cloudTemplateId;

    @Column(name = "OBJECT_ID")
    private Long objectId;

    @Column(name = "JOB_BATCH_NAME")
    private String jobBatchName;

    @Column(name = "JOB_STATUS")
    private String jobStatus;

    @Column(name = "LOAD_REQUEST_ID")
    private Long loadRequestId;

    @Column(name = "STANDARD_REQUEST_ID")
    private Long standardRequestId;

    @Column(name = "ERROR_MESSAGE")
    private String errorMessage;

    @Column(name = "JOB_START_TIME")
    private LocalDateTime jobStartTime;

    @Column(name = "JOB_END_TIME")
    private LocalDateTime jobEndTime;

}

