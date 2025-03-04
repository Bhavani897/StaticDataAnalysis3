package com.rite.products.convertrite.po;

import java.util.List;

import com.rite.products.convertrite.model.SourceTemplateHeaders;

public class SavingSourceTemplateHeadersResPo {
    private List<SourceTemplateHeaders> sourceTemplateHeaders;
    private String errorMessage;
    private String message;

    public List<SourceTemplateHeaders> getSourceTemplateHeaders() {
        return sourceTemplateHeaders;
    }

    public void setSourceTemplateHeaders(List<SourceTemplateHeaders> sourceTemplateHeaders) {
        this.sourceTemplateHeaders = sourceTemplateHeaders;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
