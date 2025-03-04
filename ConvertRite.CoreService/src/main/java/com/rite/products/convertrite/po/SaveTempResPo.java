package com.rite.products.convertrite.po;

public class SaveTempResPo {
    private SaveTemplateHeaderResponsePo saveTemplateHeaderResponsePo;
    private SaveCloudTemplateColumnsResPo saveCloudTemplateColumnsResPo;
    private String message;
    private String errorMessage;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public SaveTemplateHeaderResponsePo getSaveTemplateHeaderResponsePo() {
        return saveTemplateHeaderResponsePo;
    }

    public void setSaveTemplateHeaderResponsePo(SaveTemplateHeaderResponsePo saveTemplateHeaderResponsePo) {
        this.saveTemplateHeaderResponsePo = saveTemplateHeaderResponsePo;
    }

    public SaveCloudTemplateColumnsResPo getSaveCloudTemplateColumnsResPo() {
        return saveCloudTemplateColumnsResPo;
    }

    public void setSaveCloudTemplateColumnsResPo(SaveCloudTemplateColumnsResPo saveCloudTemplateColumnsResPo) {
        this.saveCloudTemplateColumnsResPo = saveCloudTemplateColumnsResPo;
    }
}
