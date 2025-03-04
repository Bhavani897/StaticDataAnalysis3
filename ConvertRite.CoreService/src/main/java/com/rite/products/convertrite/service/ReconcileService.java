package com.rite.products.convertrite.service;

import java.util.List;

import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.XxrReconcileConfig;
import com.rite.products.convertrite.model.XxrReconcileStatistics;
import com.rite.products.convertrite.po.ReconcileDetailsReqPo;
import com.rite.products.convertrite.po.ReconcileDetailsResPo;
import com.rite.products.convertrite.po.ReconcileErrorReqPo;
import com.rite.products.convertrite.po.ReconcileErrorResPo;
import com.rite.products.convertrite.po.SaveReconcileConfigReqPo;
import com.rite.products.convertrite.po.SaveReconcileConfigResPo;

public interface ReconcileService {

	XxrReconcileStatistics getReconcileStatistics(String cldTemplateName, String batchName);

	ReconcileDetailsResPo getReconcileDetails(ReconcileDetailsReqPo reconcileDetailsReqPo) throws Exception;

	ReconcileErrorResPo getReconcileErrors(ReconcileErrorReqPo reconcileErrorReqPo)throws Exception;

	SaveReconcileConfigResPo saveReconcileConfig(SaveReconcileConfigReqPo saveReconcileConfigReqPo)throws ValidationException;

	List<XxrReconcileConfig> getReconcileConfig();

}
