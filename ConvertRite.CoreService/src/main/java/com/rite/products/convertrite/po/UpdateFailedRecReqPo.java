package com.rite.products.convertrite.po;

import java.util.List;

public class UpdateFailedRecReqPo {

	private List<String> editedFailedRecs;
	private String cloudTemplateName;

	public List<String> getEditedFailedRecs() {
		return editedFailedRecs;
	}

	public void setEditedFailedRecs(List<String> editedFailedRecs) {
		this.editedFailedRecs = editedFailedRecs;
	}

	public String getCloudTemplateName() {
		return cloudTemplateName;
	}

	public void setCloudTemplateName(String cloudTemplateName) {
		this.cloudTemplateName = cloudTemplateName;
	}

	

}
