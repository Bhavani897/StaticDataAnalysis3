package com.rite.products.convertrite.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

import javax.persistence.*;
import java.sql.Date;
@Data
@Entity
@Table(name = "CR_SUPPLIER_TAX_PROFILE_ERRORS")
public class CrSupplierTaxProfileErrors {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "ID")
    private Long id;
    @Column(name = "PARTY_TAX_PROFILE_ID")
    private String partyTaxProfileId;
    @Column(name = "CR_BATCH_NAME")
    private String crBatchName;
    @Column(name = "STATUS_CODE")
    private String statusCode;
    @Column(name = "ERROR_MESSAGE")
    private String errorMessage;
    @Column(name = "CREATED_DATE")
    private Date createdDate;
    @Column(name="CREATED_BY")
    private String createdBy;
}
