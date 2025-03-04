package com.rite.products.convertrite.model;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Entity
@Table(name = "CR_CLD_TRANSFORM_STATS_V")
public class CrCldTransformStatsView {
    @Id
    @Column(name = "CLOUD_TEMPLATE_ID")
    private Long cloudTemplateId;
    @Column(name = "CLOUD_TEMPLATE_NAME")
    private String cloudTemplateName;
    @Column(name = "SRC_STG_TABLE_NAME")
    private String sourceStagingTableName;
    @Column(name = "CLD_STG_TABLE_NAME")
    private String cloudStagingTableName;
    @Column(name = "PROJECT_ID")
    private Long projectId;
    @Column(name = "PROJECT")
    private String projectName;
    @Column(name = "PARENT_OBJECT_ID")
    private Long parentObjectId;
    @Column(name = "PARENT_OBJECT_CODE")
    private String parentObectCode;
    @Column(name = "OBJECT_ID")
    private Long objectId;
    @Column(name = "OBJECT")
    private String objectCode;
    @Column(name = "TOTAL_SOURCE_RECORDS")
    private Integer totalSourceRecords;
    @Column(name = "TOTAL_CLOUD_RECORDS")
    private Integer totalCloudRecords;
    @Column(name = "VALIDATION_FAILED")
    private Integer validationFailed;
    @Column(name = "VALIDATION_PASSED")
    private Integer validationPassed;
    @Column(name = "SOURCE_UNVERFIED")
    private Integer sourceUnverified;
    @Column(name = "VS_TRANS_REC")
    private Integer vsTransRec;
    @Column(name = "VS_UNTRANS_REC")
    private Integer vsUntransRec;
    @Column(name = "VF_TRANS_REC")
    private Integer vfTransRec;
    @Column(name = "VF_UNTRANS_REC")
    private Integer vfUntransRec;
    @Column(name = "CLOUD_UNKOWN_RECORDS")
    private Integer cloudUnknownRecords;
    @Column(name = "TOTAL_UNTRANS_REC")
    private Integer totalUnTransRec;
    @Column(name = "cloud_int_rej_rec")
    private Integer cloudIntRejRec;
    @Column(name = "cloud_success_rec")
    private Integer cloudSuccessRec;
    @Column(name = "cr_batch_name")
    private String batchName;
    @Column(name = "load_request_id")
    private String loadRequestId;
}
