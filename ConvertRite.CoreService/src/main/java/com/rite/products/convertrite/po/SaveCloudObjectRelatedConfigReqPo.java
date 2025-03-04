package com.rite.products.convertrite.po;

public class SaveCloudObjectRelatedConfigReqPo {

	private String objectCode;
	private String ctlFileName;
	private String xmlsmFileName;
	private String sheetName;
	private String interfaceTableName;
	private String rejectionTableName;
	private String essJobSatusColumn;
	
	

	public String getEssJobSatusColumn() {
		return essJobSatusColumn;
	}

	public void setEssJobSatusColumn(String essJobSatusColumn) {
		this.essJobSatusColumn = essJobSatusColumn;
	}

	public String getRejectionTableName() {
		return rejectionTableName;
	}

	public void setRejectionTableName(String rejectionTableName) {
		this.rejectionTableName = rejectionTableName;
	}

	public String getObjectCode() {
		return objectCode;
	}

	public void setObjectCode(String objectCode) {
		this.objectCode = objectCode;
	}

	public String getCtlFileName() {
		return ctlFileName;
	}

	public void setCtlFileName(String ctlFileName) {
		this.ctlFileName = ctlFileName;
	}

	public String getXmlsmFileName() {
		return xmlsmFileName;
	}

	public void setXmlsmFileName(String xmlsmFileName) {
		this.xmlsmFileName = xmlsmFileName;
	}

	public String getSheetName() {
		return sheetName;
	}

	public void setSheetName(String sheetName) {
		this.sheetName = sheetName;
	}

	public String getInterfaceTableName() {
		return interfaceTableName;
	}

	public void setInterfaceTableName(String interfaceTableName) {
		this.interfaceTableName = interfaceTableName;
	}

}
