package com.rite.products.convertrite.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

import javax.persistence.*;
import java.io.Serializable;
import java.sql.Date;

@Data
@Entity
@Table(name = "CR_PROJ_ACTIVITIES")
public class CrProjectActivities  {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    @Column(name = "TASK_ID")
    private Long taskId;
    @Column(name = "PROJECT_ID")
    private Long projectId;

    @Column(name = "SEQ")
    private Integer seq;

    @Column(name = "TASK_NUM")
    private String taskNum;
    @Column(name = "TASK_NAME")
    private String taskName;
    @Column(name = "OBJECT_ID")
    private long objectId;

    @Column(name = "TASK_TYPE")
    private String taskType;
    @Column(name = "PRE_REQ_TASK")
    private String preReqTask;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
    @Column(name = "START_DATE")
    private Date startDate;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
    @Column(name = "END_DATE")
    private Date endDate;
    @Column(name = "WEIGHTAGE")
    private Integer weightage;
    /*
     * @Column(name = "COMPLETED_FLAG") private String taskStatus;
     */
    @Column(name = "COMPLETE_PERCENTAGE")
    private Integer completePercentage;
    @Column(name = "LEGACY_RESOURCE_ID")
    private Long legacyResourceId;
    @Column(name = "TASK_STATUS")
    private String taskStatus;
    @Column(name = "DESTINATION_RESOURCE_ID")
    private Long destinationResourceId;
    @Column(name = "TASK_OWNER_ID")
    private Long taskOwnerId;
    @Column(name = "COMPLETION_FLAG")
    private String completionFlag;
    @Column(name = "CLOUD_RESOURCE_ID")
    private Long cloudResourceId;
    @Column(name = "INTEGRATOR_RESOURCE_ID")
    private Long integratorResourceId;
    @Column(name = "CLIENT_RESOURCE_ID")
    private Long clientResourceId;
    @Column(name = "LAST_UPDATED_BY")
    private String lastUpdatedBy;


    @Column(name = "LAST_UPDATE_DATE")
    private Date lastUpdateDate;
    @Column(name = "CREATION_DATE")
    private String creationDate;
    @Column(name = "CREATED_BY")
    private Date createdBy;

    @Column(name = "ATTRIBUTE1")
    private String attribute1;
    @Column(name = "ATTRIBUTE2")
    private String attribute2;

}
