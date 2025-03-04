package com.rite.products.convertrite.po;

import java.util.Date;

public class SourceLoadFailedRecords {

    private Long id;
    private Long  templateId;
    private String templateName;
    private String parentObjectCode;
    private String fileName;
    private Long success;
    private Long failed;
    private String createdBy;
    private Date creationDate;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getTemplateId() {
        return templateId;
    }

    public void setTemplateId(Long templateId) {
        this.templateId = templateId;
    }

    public String getTemplateName() {
        return templateName;
    }

    public void setTemplateName(String templateName) {
        this.templateName = templateName;
    }

    public String getParentObjectCode() {
        return parentObjectCode;
    }

    public void setParentObjectCode(String parentObjectCode) {
        this.parentObjectCode = parentObjectCode;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public Long getSuccess() {
        return success;
    }

    public void setSuccess(Long success) {
        this.success = success;
    }

    public Long getFailed() {
        return failed;
    }

    public void setFailed(Long failed) {
        this.failed = failed;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public Date getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(Date creationDate) {
        this.creationDate = creationDate;
    }

    public SourceLoadFailedRecords(Long id, Long templateId, String templateName, String parentObjectCode, String fileName, Long success, Long failed, String createdBy, Date creationDate) {
        this.id = id;
        this.templateId = templateId;
        this.templateName = templateName;
        this.parentObjectCode = parentObjectCode;
        this.fileName = fileName;
        this.success = success;
        this.failed = failed;
        this.createdBy = createdBy;
        this.creationDate = creationDate;
    }
}