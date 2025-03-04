package com.rite.products.convertrite.po;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ObjectsPo {
    public Integer objectId;
    public String objectName;
    public String objectCode;
    public String userObjectName;
    public String moduleCode;
    public Integer parentObjectId;
    public String fbdiSheet;
    public String hdlSheet;
    public String loaderEndpoint;
    public String reConQuery;
    public String sequenceInParent;
    public String creationDate;
    public String createdBy;
    public String lastUpdatedDate;
    public String lastUpdatedBy;
    public String conversionType;
    private String insertTableName;
    private String cldTemplateCode;
    private String cldMetaDataTableName;
    private String baseTables;
    private String cldTemplateName;
}
