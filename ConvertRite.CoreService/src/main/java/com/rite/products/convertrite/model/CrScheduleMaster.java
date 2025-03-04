package com.rite.products.convertrite.model;

import lombok.Data;

import javax.persistence.*;
import java.util.Date;

@Entity
@Table(name = "CR_SCHEDULE_MASTER")
@Data
public class CrScheduleMaster {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "SCHEDULE_ID", nullable = false)
    private Long scheduleId;

    @Column(name = "SCHEDULE_NAME", nullable = false, length = 255)
    private String scheduleName;

    @Column(name = "SOURCE_TEMPLATE_IDS_LIST", length = 500)
    private String sourceTemplateIdsList;

    @Column(name = "BATCH_NAME", length = 255)
    private String batchName;

    @Column(name = "SCHEDULE_FREQUENCY", length = 100)
    private Long scheduleFrequency;

    @Column(name = "SOURCE_SYSTEM_ID", length = 100)
    private Long sourceSystemId;

    @Column(name = "TARGET_SYSTEM_ID", length = 100)
    private Long targetSystemId;

    @Column(name = "EXTRACTION_TYPE", length = 50)
    private String extractionType;

    @Column(name = "SCHEDULE_START_TIME")
    private Date scheduleStartTime;

    @Column(name = "SCHEDULE_END_TIME")
    private Date scheduleEndTime;

    @Column(name = "STATUS", length = 50)
    private String status;

    @Column(name = "CREATED_BY", length = 255)
    private String createdBy;

    @Column(name = "CREATED_DATE")
    private Date createdDate;

    @Column(name = "LAST_UPDATED_BY", length = 255)
    private String lastUpdatedBy;

    @Column(name = "LAST_UPDATED_DATE")
    private Date lastUpdatedDate;

    @Column(name = "AUTH_TOKEN")
    private String authToken;

    @Column(name = "IS_RELATIONAL")
    private String isDependent;

    @Column(name = "FILE_PATH")
    private String filePath;
}
