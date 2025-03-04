package com.rite.products.convertrite.service;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.*;
import com.rite.products.convertrite.po.*;
import com.rite.products.convertrite.respository.*;
import com.rite.products.convertrite.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import javax.persistence.EntityManager;
import javax.persistence.ParameterMode;
import javax.persistence.PersistenceContext;
import javax.persistence.StoredProcedureQuery;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class CrConversionServiceImpl implements CrConversionService {
	@PersistenceContext
	private EntityManager entityManager;
	private static final Logger log = LoggerFactory.getLogger(CrConversionServiceImpl.class);
	@Value("${covertrite-core-custom-restapi}")
	private String customRestApi;
	@Autowired
	CrTemplateStateRepository crTemplateStateRepository;
	@Autowired
	CloudTemplateHeaderDaoImpl cloudTemplateHeaderDaoImpl;
	@Autowired
	CrTemplateStatisticsViewRepository crTemplateStatisticsViewRepository;

	@Autowired
	CrConversionDaoImpl crConversionDaoImpl;

	@Autowired
	CrProcessRequestsViewRepository crProcessRequestsViewRepository;

	@Autowired
	CrCloudTemplateHeadersRepository crCloudTemplateHeadersRepository;
	@Autowired
	CrLookUpValuesRepo crLookUpValuesRepo;
	@Autowired
	CrCloudTemplateHeadersViewRepository crCloudTemplateHeadersViewRepository;

	@Autowired
	CrUserHooksRepo crUserHooksRepo;

	@Autowired
	CrConversionService crConversionService;

	@Autowired
	Utils utils;

	@Override
	public List<CrTemplateStateView> getTemplateState() throws Exception {
		return crTemplateStateRepository.findAll();
	}

	@Override
	public List<CrTemplateStatisticsResPo> getTemplateStatistics() throws Exception {
		List<CrTemplateStatisticsResPo> templateStatistic = new ArrayList<>();
		Map<String, List<CrTemplateStatisticsView>> dataByTemplate = crTemplateStatisticsViewRepository.findAll().stream()
				.collect(Collectors.groupingBy(CrTemplateStatisticsView::getCriteriaType));

		CrTemplateStatisticsResPo templateRes = new CrTemplateStatisticsResPo();
		templateRes.setCriteriaType("TEMPLATE");
		templateRes.setData(dataByTemplate.get("TEMPLATE"));
		CrTemplateStatisticsResPo podRes = new CrTemplateStatisticsResPo();
		podRes.setData(dataByTemplate.get("POD"));
		podRes.setCriteriaType("POD");
		CrTemplateStatisticsResPo projectRes = new CrTemplateStatisticsResPo();
		projectRes.setData(dataByTemplate.get("PROJECT"));
		projectRes.setCriteriaType("PROJECT");
		CrTemplateStatisticsResPo objectRes = new CrTemplateStatisticsResPo();
		objectRes.setData(dataByTemplate.get("OBJECT"));
		objectRes.setCriteriaType("OBJECT");
		CrTemplateStatisticsResPo parentObjectRes = new CrTemplateStatisticsResPo();
		parentObjectRes.setData(dataByTemplate.get("PARENT_OBJECT_CODE"));
		parentObjectRes.setCriteriaType("PARENT_OBJECT_CODE");

		templateStatistic.add(templateRes);
		templateStatistic.add(podRes);
		templateStatistic.add(projectRes);
		templateStatistic.add(objectRes);
		templateStatistic.add(parentObjectRes);

		return templateStatistic;
	}


	@Override
	public List<CrProcessRequestsView> getProcessRequests(CrProcessRequestsPagePo crProcessRequestsPagePo,
														  HttpHeaders httpHeaders) throws Exception {
		List<CrProcessRequestsView> processRequestLi = new ArrayList<>();
		Page<CrProcessRequestsView> page = null;
		Pageable pageable = PageRequest.of(crProcessRequestsPagePo.getPageNo(), crProcessRequestsPagePo.getPageSize(),
				Sort.by(crProcessRequestsPagePo.getSortDirection(), crProcessRequestsPagePo.getSortBy()));
		if (!Validations.isNullOrEmpty(crProcessRequestsPagePo.getBatchName()))
			page = crProcessRequestsViewRepository.findAllByBatchName(crProcessRequestsPagePo.getBatchName(),
					pageable);
		else
			page = crProcessRequestsViewRepository.findAll(pageable);
		httpHeaders.set("pagecount", String.valueOf(page.getTotalPages()));
		httpHeaders.set("totalcount", String.valueOf(page.getTotalElements()));

		if (page.hasContent())
			processRequestLi = page.getContent();
		return processRequestLi;
	}

	@Override
	public ProcessJobPo processJobV1(String cloudTemplateName, String type, String batchName,
									 HttpServletRequest request) throws Exception {
		//TODO
		//return processJobDaoImpl.processJobV1(cloudTemplateName, type, batchName, request);
		return new ProcessJobPo();
	}

	@Override
	public void downloadFbdi(Long cloudTemplateId, String batchName, HttpServletResponse response) throws Exception {
			cloudTemplateHeaderDaoImpl.downloadFbdi(cloudTemplateId, batchName,response);
	}

	@Override
	public ResponseEntity<Object> transformDataToCloud(String cloudTemplateName, String pReprocessFlag, String pBatchFlag, String pBatchName, String tenantId,String userId) throws Exception {
		Map<String,String> resMap=new HashMap<>();

		resMap=crConversionDaoImpl.transformDataToCloud(cloudTemplateName,pReprocessFlag,pBatchFlag,pBatchName,userId);
		CrCloudTemplateHeadersView cldTemplateHeaders=crCloudTemplateHeadersViewRepository.findByTemplateName(cloudTemplateName);
		if ( cldTemplateHeaders!= null) {
			CrUserHookResPo crUserHookResPo=crUserHooksRepo.fndByHookType(cldTemplateHeaders.getTemplateId());
			if(crUserHookResPo!=null) {
				if (!Validations.isNullOrEmptyorWhiteSpace(crUserHookResPo.getHookText())) {
					log.info("entering into userhook requestId {}",resMap.get("requestId"));
					Long requestId=Long.parseLong(resMap.get("requestId"));
					Long podId=Long.parseLong(tenantId);
					String status = utils.getProcessStatus(requestId,tenantId);
					// post_validation API hook calling
					if ("C".equalsIgnoreCase(status)) {
						Map<String, String> objectInfos = new HashMap<>();
						String token = getTheToken();
						CrObjectInformationPo crObjectInformationPo=utils.getObjectsWithInformation(podId,cldTemplateHeaders.getObjectId().toString(),token);
						if(crObjectInformationPo.getObjectInfoWithPodClodConfigPos().isEmpty())
							throw new ValidationException("Object Information or Pod Cloud config info is empty");
						for (CrObjectInformation objectInformation : crObjectInformationPo.getObjectInfoWithPodClodConfigPos().get(0).getCrObjectInformation()) {
							objectInfos.put(objectInformation.getInfoType(), objectInformation.getInfoValue());
						}
						CustomRestApiReqPo customRestApiReqPo = new CustomRestApiReqPo();
						customRestApiReqPo.setCldTemplateId(cldTemplateHeaders.getTemplateId());
						customRestApiReqPo.setBatchName(pBatchName);
						customRestApiReqPo.setObjectName(cldTemplateHeaders.getObjectCode());
						customRestApiReqPo.setObjectId(cldTemplateHeaders.getObjectId());
						customRestApiReqPo.setCldUserName(crObjectInformationPo.getCloudLoginDetails().get(0).getUsername());
						customRestApiReqPo.setCldPassword(crObjectInformationPo.getCloudLoginDetails().get(0).getPassword());
						customRestApiReqPo.setPodId(podId);
						customRestApiReqPo.setCcidColumnName(objectInfos.get("CCID Column Name"));
						customRestApiReqPo.setLedgerColumnName(objectInfos.get("Ledger Column Name"));
						customRestApiReqPo.setCloudUrl(crObjectInformationPo.getCloudLoginDetails().get(0).getUrl());
						customRestApiReqPo.setRestApiUrl(crUserHookResPo.getHookText());
						customRestApiReqPo.setRequestId(requestId);
						List<CrProcessRequestsView> res;
						while (true) {
							res = crProcessRequestsViewRepository.findAllByObjectId(cldTemplateHeaders.getObjectId());
							if (!res.isEmpty() && res.get(0).getJobStatus().equals("C") && res.get(0).getRequestId() != null) {
								updateParamlistProc(pBatchName, cldTemplateHeaders.getTemplateId());
								break;
							}
						}

						ValidateUserHook(customRestApiReqPo);
					}
				}
			}
		}
		return new ResponseEntity<Object>(resMap, HttpStatus.OK);
	}


	private void updateParamlistProc(String batchName, Long cldTemplateId){
		StoredProcedureQuery updatingPareamLst = entityManager
				.createStoredProcedureQuery("r_supplier_update_paramlist_proc")
				.registerStoredProcedureParameter("p_batch_name", String.class, ParameterMode.IN)
				.registerStoredProcedureParameter("p_cld_template_id", String.class, ParameterMode.IN)
                .setParameter("p_batch_name", batchName)
				.setParameter("p_cld_template_id", cldTemplateId);
		updatingPareamLst.execute();
	}

	private String getTheToken() {
		String token="eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJyaXRlYWRtaW4iLCJyb2xlcyI6IlJPTEVfUklURUFETUlOIiwiaWF0IjoxNzM2MjI4MjYwLCJleHAiOjE3MzYyMzkwNjB9.3MHbEvNgTTMsprwI-Nd1pf5dYqewjLCfq08lowuvsb4";
	return token;
	}


	private void ValidateUserHook(CustomRestApiReqPo customRestApiReqPo) {
		log.info("Start of ValidateUserHook Method -->");
		if(customRestApiReqPo!=null) {
			String restApiUrl =customRestApi+customRestApiReqPo.getRestApiUrl();
			log.info(restApiUrl + " :ValidateUserHookApiUrl");
			HttpHeaders headers = new HttpHeaders();
			headers.set("Accept", MediaType.APPLICATION_JSON_VALUE);
			headers.set("X-TENANT-ID", String.valueOf(customRestApiReqPo.getPodId()));
			//Calling custom RESTAPI for Post validation user hook
			HttpEntity<CustomRestApiReqPo> requestEntity = new HttpEntity<>(customRestApiReqPo,
					headers);
			RestTemplate restTemplate = new RestTemplate();
			restTemplate.exchange(restApiUrl, HttpMethod.POST, requestEntity, String.class);
		}
	}

	@Override
	public void generateHdlFromLob(String cloudTemplateId, String batchName, String isIntialLoad, HttpServletResponse response)
			throws Exception {
		log.info("Start Of generateHdlFromLob in service###");

		// Split the cloudTemplateIds by comma
		String[] cloudTemplateIdArray = cloudTemplateId.split(",");

		log.info("First cloudTemplateId: " + Long.valueOf(cloudTemplateIdArray[0]));

		// Initialize variables
		Long parentObjectId = null;
		String parentObjectCode = "";

		// Set response content type
		response.setContentType("text/dat");
		// Set response header
		response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=combinedOutput.dat");

		// Get the PrintWriter for writing the response
		PrintWriter writer = response.getWriter();

			CrCloudTemplateHeaders crCloudTemplateHeader = crCloudTemplateHeadersRepository
					.findById(Long.valueOf(cloudTemplateIdArray[0])).orElse(null);

			if (crCloudTemplateHeader != null) {
				parentObjectId = crCloudTemplateHeader.getParentObjectId();
				if (parentObjectId != null) {
					parentObjectCode = crLookUpValuesRepo.getValueById(parentObjectId);
				}
			}

		response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + parentObjectCode + ".dat");
		// Call the method to generate HDL and pass the writer to append the output
		cloudTemplateHeaderDaoImpl.generateHdlFromLob(cloudTemplateId, batchName, isIntialLoad, writer);
		// Close the writer after all IDs are processed
		writer.flush();
		writer.close();
	}

	@Override
	public BasicResPo insertTransformStats(String userId, Long cloudTemplateId, String batchName) throws Exception {
		BasicResPo resPo = new BasicResPo();

		log.info("Start of insertTransformStats Method ######");
		StoredProcedureQuery createStaggingStoredProcedure = entityManager
				.createStoredProcedureQuery("cr_fetch_transform_stats_proc")
				.registerStoredProcedureParameter("p_user_id", String.class, ParameterMode.IN)

				.registerStoredProcedureParameter("p_cld_template_id", Long.class, ParameterMode.IN)
				.registerStoredProcedureParameter("p_batch_name", String.class, ParameterMode.IN)
				.registerStoredProcedureParameter("p_ret_code", String.class, ParameterMode.OUT)
				.registerStoredProcedureParameter("p_ret_msg", String.class, ParameterMode.OUT);

		createStaggingStoredProcedure
				.setParameter("p_user_id", userId)
				.setParameter("p_cld_template_id", cloudTemplateId)
				.setParameter("p_batch_name", batchName);

		createStaggingStoredProcedure.execute();

		String retCode = (String) createStaggingStoredProcedure.getOutputParameterValue("p_ret_code");
		String retMsg = (String) createStaggingStoredProcedure.getOutputParameterValue("p_ret_msg");


		entityManager.clear();
		entityManager.close();
		resPo = new BasicResPo() {{
			setStatusCode(HttpStatus.OK);
			setStatus(retCode);
			setMessage(retMsg);
			setPayload(null);
		}};


		return resPo;
	}

}

