package com.rite.products.convertrite.po;

import com.rite.products.convertrite.model.XxrCloudConfig;

import java.util.List;

public class SaveCloudAtomationRes {
    private List<XxrCloudConfig> xxrCloudConfigs;
    private SaveLookUpValuesResPo saveParentLookUps;
    private SaveLookUpValuesResPo saveObjectLookups;
    private SaveTemplateHeaderResponsePo saveTemplateHeaderResponsePo;
    private SaveFbdiTemplateColumnsResPo saveFbdiTemplateColumnsResPo;
    private SaveTemplateHeaderResponsePo saveCloudTempHeaderResponsePo;
    private SaveCloudTemplateColumnsResPo saveCloudTemplateColumnsResPo;
//    private RoleObjectLinkResPo roleObjectLinkResPo;
    private String errMessage;

    public String getErrMessage() {
        return errMessage;
    }

    public void setErrMessage(String errMessage) {
        this.errMessage = errMessage;
    }

//    public RoleObjectLinkResPo getRoleObjectLinkResPo() {
//        return roleObjectLinkResPo;
//    }
//
//    public void setRoleObjectLinkResPo(RoleObjectLinkResPo roleObjectLinkResPo) {
//        this.roleObjectLinkResPo = roleObjectLinkResPo;
//    }

    public SaveFbdiTemplateColumnsResPo getSaveFbdiTemplateColumnsResPo() {
        return saveFbdiTemplateColumnsResPo;
    }

    public void setSaveFbdiTemplateColumnsResPo(SaveFbdiTemplateColumnsResPo saveFbdiTemplateColumnsResPo) {
        this.saveFbdiTemplateColumnsResPo = saveFbdiTemplateColumnsResPo;
    }

    public SaveTemplateHeaderResponsePo getSaveTemplateHeaderResponsePo() {
        return saveTemplateHeaderResponsePo;
    }

    public void setSaveTemplateHeaderResponsePo(SaveTemplateHeaderResponsePo saveTemplateHeaderResponsePo) {
        this.saveTemplateHeaderResponsePo = saveTemplateHeaderResponsePo;
    }

    public List<XxrCloudConfig> getXxrCloudConfigs() {
        return xxrCloudConfigs;
    }

    public void setXxrCloudConfigs(List<XxrCloudConfig> xxrCloudConfigs) {
        this.xxrCloudConfigs = xxrCloudConfigs;
    }


    public SaveLookUpValuesResPo getSaveParentLookUps() {
        return saveParentLookUps;
    }

    public void setSaveParentLookUps(SaveLookUpValuesResPo saveParentLookUps) {
        this.saveParentLookUps = saveParentLookUps;
    }

    public SaveLookUpValuesResPo getSaveObjectLookups() {
        return saveObjectLookups;
    }

    public void setSaveObjectLookups(SaveLookUpValuesResPo saveObjectLookups) {
        this.saveObjectLookups = saveObjectLookups;
    }

    public SaveTemplateHeaderResponsePo getSaveCloudTempHeaderResponsePo() {
        return saveCloudTempHeaderResponsePo;
    }

    public void setSaveCloudTempHeaderResponsePo(SaveTemplateHeaderResponsePo saveCloudTempHeaderResponsePo) {
        this.saveCloudTempHeaderResponsePo = saveCloudTempHeaderResponsePo;
    }

    public SaveCloudTemplateColumnsResPo getSaveCloudTemplateColumnsResPo() {
        return saveCloudTemplateColumnsResPo;
    }

    public void setSaveCloudTemplateColumnsResPo(SaveCloudTemplateColumnsResPo saveCloudTemplateColumnsResPo) {
        this.saveCloudTemplateColumnsResPo = saveCloudTemplateColumnsResPo;
    }
}
