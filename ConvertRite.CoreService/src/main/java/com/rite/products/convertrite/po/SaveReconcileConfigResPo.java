package com.rite.products.convertrite.po;

import com.rite.products.convertrite.model.XxrReconcileConfig;

public class SaveReconcileConfigResPo {
	private String message;
	private XxrReconcileConfig xxrReconcileConfig;
	private String error;

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public XxrReconcileConfig getXxrReconcileConfig() {
		return xxrReconcileConfig;
	}

	public void setXxrReconcileConfig(XxrReconcileConfig xxrReconcileConfig) {
		this.xxrReconcileConfig = xxrReconcileConfig;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

}
