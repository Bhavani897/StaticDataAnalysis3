package com.rite.products.convertrite.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

import javax.persistence.*;
import java.sql.Date;

@Data
@Entity
@Table(name = "CR_PROJ_DFF_ERROR")
public class CrProjDffError {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "ID")
    private Long id;
    @Column(name = "PROJECT_ID")
    private String projectId;
    @Column(name = "PARENT_PROJECT_ID")
    private String parentProjectId;
    @Column(name = "PARENT_PROJECT_NUMBER")
    private String parentProjectNumber;
    @Column(name = "CR_BATCH_NAME")
    private String crBatchName;
    @Column(name = "ERROR_MESSAGE")
    private String errorMessage;
    @Column(name = "CREATION_DATE")
    private Date creationDate;
    @Column(name = "CREATED_BY")
    private String createdBy;

}
