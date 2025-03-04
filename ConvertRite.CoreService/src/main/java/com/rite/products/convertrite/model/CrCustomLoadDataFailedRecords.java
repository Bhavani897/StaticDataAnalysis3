package com.rite.products.convertrite.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

import javax.persistence.*;
import java.util.Date;

@Data
@Entity
@Table(name = "CR_CUSTOM_LOADDATA_FAIL_RECORDS")
public class CrCustomLoadDataFailedRecords {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "RECORD_ID")
    private Long id;
    @Column(name = "CUSTOM_TABLE_ID")
    private Long customTableId;
    @Column(name = "FILE_NAME")
    private String fileName;
    @Lob
    @Column(name = "FAILED_CLOB")
    private String failedClob;
    @Column(name = "SUCCESS_COUNT")
    private long success;
    @Column(name = "FAILED_COUNT")
    private long failed;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
    @Column(name = "CREATION_DATE")
    private Date creationDate;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
    @Column(name = "LAST_UPDATE_DATE")
    private Date lastUpdatedDate;
    @Column(name = "CREATED_BY")
    private String createdBy;
    @Column(name = "LAST_UPDATED_BY")
    private String lastUpdateBy;
    @Lob
    @Column(name = "LOG_FILE_BLOB")
    private String logFileBlob;
    @Column(name = "CR_BATCH_NAME")
    private String crBatchName;
}
