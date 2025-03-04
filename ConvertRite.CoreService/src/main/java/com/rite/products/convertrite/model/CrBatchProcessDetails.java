package com.rite.products.convertrite.model;

import lombok.Data;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Date;

@Entity
@Data
@Table(name = "CR_BATCH_PROCESS_DETAILS")
public class CrBatchProcessDetails {
    @Id
    private Long srcTempId;
    private Long objectId;
    private Long sourceCount;
    private String batchName;
    private Long noOfBatchSplit;
    private String batchNamePostSplit;
    private String duplicateBatchName;
    private Long duplicateCount;
    private Long totalFurProcessRecords;
    private String attribute1;
    private String attribute2;
    private String attribute3;
    private String attribute4;
    private String attribute5;
    private String createdBy;
    private Date createdDate;
    private String updatedBy;
    private Date updatedDate;

    public CrBatchProcessDetails(Long srcTempId, Long objectId, Long sourceCount, String batchName, Long noOfBatchSplit, String batchNamePostSplit, String duplicateBatchName, Long duplicateCount, Long totalFurProcessRecords, String attribute1, String attribute2, String attribute3, String attribute4, String attribute5, String createdBy, Date createdDate, String updatedBy, Date updatedDate) {
        this.srcTempId = srcTempId;
        this.objectId = objectId;
        this.sourceCount = sourceCount;
        this.batchName = batchName;
        this.noOfBatchSplit = noOfBatchSplit;
        this.batchNamePostSplit = batchNamePostSplit;
        this.duplicateBatchName = duplicateBatchName;
        this.duplicateCount = duplicateCount;
        this.totalFurProcessRecords = totalFurProcessRecords;
        this.attribute1 = attribute1;
        this.attribute2 = attribute2;
        this.attribute3 = attribute3;
        this.attribute4 = attribute4;
        this.attribute5 = attribute5;
        this.createdBy = createdBy;
        this.createdDate = createdDate;
        this.updatedBy = updatedBy;
        this.updatedDate = updatedDate;
    }

}
