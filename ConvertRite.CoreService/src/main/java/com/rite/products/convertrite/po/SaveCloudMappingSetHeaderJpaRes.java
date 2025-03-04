package com.rite.products.convertrite.po;

import java.util.List;

import com.rite.products.convertrite.model.XxrMappingSets;

public class SaveCloudMappingSetHeaderJpaRes {
    private String message;
    private List<XxrMappingSets> xxrMappingSetsList;
    private String error;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public List<XxrMappingSets> getXxrMappingSetsList() {
        return xxrMappingSetsList;
    }

    public void setXxrMappingSetsList(List<XxrMappingSets> xxrMappingSetsList) {
        this.xxrMappingSetsList = xxrMappingSetsList;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }
}
