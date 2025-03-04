package com.rite.products.convertrite.model;


import java.sql.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

@Data
@Entity
@Table(name = "CR_CREATE_BANK_BRANCH_ERRORS")
public class CrCreateBankBranchErrors {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "ID")
    private Long id;
    @Column(name = "BANK_NAME")
    private String bankName;
    @Column(name = "BANK_NUMBER")
    private String bankNumber;
    @Column(name = "BRANCH_NAME")
    private String branchName;
    @Column(name = "COUNTRY_NAME")
    private String countryName;
    @Column(name = "BRANCH_NUMBER")
    private String branchNumber;
    @Column(name = "EFT_SWIFT_CODE")
    private String eftSwiftCode;
    @Column(name = "CR_BATCH_NAME")
    private String crBatchName;
    @Column(name = "CLOUD_TEMPLATE_ID")
    private Long cldTempId;
    @Column(name = "ERROR_TYPE")
    private String errorType;
    @Column(name = "STATUS")
    private String status;
    @Column(name = "ERROR_MESSAGE")
    private String errorMessage;
    @Column(name = "CREATION_DATE")
    private Date creationDate;
    @Column(name = "CREATED_BY")
    private String createdBy;
}

