package com.rite.products.convertrite.po;

public class CloudCSVConfigPojo {
    private String XlsmFileName;
    private String CtlFileName;
    private String ObjectCode;
    private String InterfaceTableName;
    private String SheetName;
    private String ParentObjectName;
    private String rejectionTableName;

    public String getXlsmFileName() {
        return XlsmFileName;
    }

    public void setXlsmFileName(String xlsmFileName) {
        XlsmFileName = xlsmFileName;
    }

    public String getCtlFileName() {
        return CtlFileName;
    }

    public void setCtlFileName(String ctlFileName) {
        CtlFileName = ctlFileName;
    }

    public String getObjectCode() {
        return ObjectCode;
    }

    public void setObjectCode(String objectCode) {
        ObjectCode = objectCode;
    }

    public String getInterfaceTableName() {
        return InterfaceTableName;
    }

    public void setInterfaceTableName(String interfaceTableName) {
        InterfaceTableName = interfaceTableName;
    }

    public String getSheetName() {
        return SheetName;
    }

    public void setSheetName(String sheetName) {
        SheetName = sheetName;
    }

    public String getParentObjectName() {
        return ParentObjectName;
    }

    public void setParentObjectName(String parentObjectName) {
        ParentObjectName = parentObjectName;
    }

    public String getRejectionTableName() {
        return rejectionTableName;
    }

    public void setRejectionTableName(String rejectionTableName) {
        this.rejectionTableName = rejectionTableName;
    }
}
