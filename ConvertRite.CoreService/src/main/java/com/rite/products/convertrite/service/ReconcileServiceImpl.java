package com.rite.products.convertrite.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.XxrReconcileConfig;
import com.rite.products.convertrite.model.XxrReconcileStatistics;
import com.rite.products.convertrite.po.ReconcileDetailsReqPo;
import com.rite.products.convertrite.po.ReconcileDetailsResPo;
import com.rite.products.convertrite.po.ReconcileErrorReqPo;
import com.rite.products.convertrite.po.ReconcileErrorResPo;
import com.rite.products.convertrite.po.SaveReconcileConfigReqPo;
import com.rite.products.convertrite.po.SaveReconcileConfigResPo;
import com.rite.products.convertrite.respository.ReconcileDaoImpl;
import com.rite.products.convertrite.respository.XxrReconcileConfigRepository;
import com.rite.products.convertrite.respository.XxrReconcileStatisticsRepository;

@Service
public class ReconcileServiceImpl implements ReconcileService {

	private static final Logger log = LoggerFactory.getLogger(ReconcileServiceImpl.class);

	@Autowired
	ReconcileDaoImpl reconcileDaoImpl;

	@Autowired
	XxrReconcileStatisticsRepository xxrReconcileStatisticsRepository;
	@Autowired
	XxrReconcileConfigRepository xxrReconcileConfigRepository;

	@Override
	public XxrReconcileStatistics getReconcileStatistics(String cldTemplateName, String batchName) {
		log.info("Start of  getReconcileStatistics in service ###");
		reconcileDaoImpl.recocileProc(cldTemplateName, batchName);
		XxrReconcileStatistics xxrReconcileStatistics = xxrReconcileStatisticsRepository
				.findByBatchNameAndCldTemplateName(batchName, cldTemplateName);
		return xxrReconcileStatistics;
	}

	@Override
	public ReconcileDetailsResPo getReconcileDetails(ReconcileDetailsReqPo reconcileDetailsReqPo) throws Exception {
		log.info("Start of getReconcileDetails Method in service ######");
		ReconcileDetailsResPo reconcileDetailsResPo = new ReconcileDetailsResPo();
		reconcileDetailsResPo = reconcileDaoImpl.getReconcileDetails(reconcileDetailsReqPo);
		return reconcileDetailsResPo;
	}

	@Override
	public ReconcileErrorResPo getReconcileErrors(ReconcileErrorReqPo reconcileErrorReqPo) throws Exception {
		log.info("Start of getReconcileErrors Method in service ######");
		ReconcileErrorResPo reconcileErrorResPo = new ReconcileErrorResPo();
		reconcileErrorResPo = reconcileDaoImpl.getReconcileErrors(reconcileErrorReqPo);
		return reconcileErrorResPo;
	}

	@Override
	public SaveReconcileConfigResPo saveReconcileConfig(SaveReconcileConfigReqPo saveReconcileConfigReqPo)
			throws ValidationException {
		log.info("Start of saveReconcileConfig Method in service ###");
		XxrReconcileConfig xxrReconcileConfig = new XxrReconcileConfig();
		SaveReconcileConfigResPo saveReconcileConfigResPo = new SaveReconcileConfigResPo();
		XxrReconcileConfig xxrReconcileConfigRes = new XxrReconcileConfig();
		if (saveReconcileConfigReqPo.getId() != null) {
			Optional<XxrReconcileConfig> xxrReconcileConfigOpt = xxrReconcileConfigRepository
					.findById(saveReconcileConfigReqPo.getId());
			if (xxrReconcileConfigOpt.isPresent()) {
				XxrReconcileConfig xxrReconcileConf = xxrReconcileConfigOpt.get();
				xxrReconcileConf.setObjectId(saveReconcileConfigReqPo.getObjectId());
				xxrReconcileConf.setObjectName(saveReconcileConfigReqPo.getObjectCode());
				xxrReconcileConf.setVersion(saveReconcileConfigReqPo.getVersion());
				xxrReconcileConf.setWhereClause(saveReconcileConfigReqPo.getWhereClause());
				xxrReconcileConf.setSqlView(saveReconcileConfigReqPo.getSqlViewQuery());
				xxrReconcileConf.setLastUpdateBy("ConvertRite");
				xxrReconcileConf.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
				xxrReconcileConfigRes = xxrReconcileConfigRepository.save(xxrReconcileConf);
			} else {
				throw new ValidationException("Reconcile Configuration is not present for the given id");
			}
		} else {
			xxrReconcileConfig.setObjectId(saveReconcileConfigReqPo.getObjectId());
			xxrReconcileConfig.setObjectName(saveReconcileConfigReqPo.getObjectCode());
			xxrReconcileConfig.setVersion(saveReconcileConfigReqPo.getVersion());
			xxrReconcileConfig.setWhereClause(saveReconcileConfigReqPo.getWhereClause());
			xxrReconcileConfig.setSqlView(saveReconcileConfigReqPo.getSqlViewQuery());
			xxrReconcileConfig.setLastUpdateBy("ConvertRite");
			xxrReconcileConfig.setCreatedBy("ConvertRite");
			xxrReconcileConfig.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
			xxrReconcileConfig.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
			xxrReconcileConfigRes = xxrReconcileConfigRepository.save(xxrReconcileConfig);
		}
		saveReconcileConfigResPo.setMessage("Saved successfully ReconcileConfigDetails");
		saveReconcileConfigResPo.setXxrReconcileConfig(xxrReconcileConfigRes);
		return saveReconcileConfigResPo;
	}

	@Override
	public List<XxrReconcileConfig> getReconcileConfig() {
		log.info("Start of getReconcileConfig Method in service ###");
		List<XxrReconcileConfig> reconcileConfigLi = new ArrayList<>();
		reconcileConfigLi = xxrReconcileConfigRepository.findAll();
		return reconcileConfigLi;
	}

}
