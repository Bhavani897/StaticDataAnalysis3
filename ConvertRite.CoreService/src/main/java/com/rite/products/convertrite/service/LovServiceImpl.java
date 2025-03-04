package com.rite.products.convertrite.service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.rite.products.convertrite.respository.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.model.XxrRoleObjectLinks;
import com.rite.products.convertrite.po.BuLovPo;
import com.rite.products.convertrite.po.CommonLovPo;
import com.rite.products.convertrite.po.LovValuesPo;
import com.rite.products.convertrite.po.ObjectCodeLovPo;
import com.rite.products.convertrite.po.ParentObjectCodeResPo;
import com.rite.products.convertrite.po.ParentObjectLovPo;
import com.rite.products.convertrite.po.PodsPo;
import com.rite.products.convertrite.po.ProjectEntryLovPo;
import com.rite.products.convertrite.po.ProjectsLovPo;
import com.rite.products.convertrite.po.ProjectsPo;
import com.rite.products.convertrite.utils.Utils;

@Service
public class LovServiceImpl implements LovService {

	@Autowired
	CloudMetaDataRepository cloudMetaDataRepository;
	@Autowired
	XxrLookUpValuesRepository xxrLookUpValuesRepository;
	@Autowired
	XxrRolesRepository xxrRolesRepository;
	@Autowired
	CrLookUpValuesRepo crLookUpValuesRepo;
	@Autowired
	XxrRoleObjectLinksRepository xxrRoleObjectLinksRepository;

	private static final Logger log = LoggerFactory.getLogger(LovServiceImpl.class);

	@Override
	public ProjectEntryLovPo getProjectEntryLovs(String[] lovValues) throws Exception {
		log.info("Start of getProjectEntryLovs ######");
		ProjectEntryLovPo projectEntryLovPo = new ProjectEntryLovPo();
		try {
			for (int i = 0; i < lovValues.length; i++) {

				if (lovValues[i].equalsIgnoreCase("PROJECT_NAME")) {
					List<LovValuesPo> projectNameList = cloudMetaDataRepository.getValues("PROJECT_NAME");
					projectEntryLovPo.setProjectName(projectNameList);
				} else if (lovValues[i].equalsIgnoreCase("PROJECT_MANAGER")) {
					List<LovValuesPo> projectMgrList = cloudMetaDataRepository.getValues("Project Manager");
					projectEntryLovPo.setProjectManager(projectMgrList);
				} else if (lovValues[i].equalsIgnoreCase("PROGRAM_NUM")) {
					List<LovValuesPo> programNumList = cloudMetaDataRepository.getValues("Program Number");
					projectEntryLovPo.setProgramNumber(programNumList);
				} else if (lovValues[i].equalsIgnoreCase("CLIENT_MANAGER")) {
					List<LovValuesPo> clientMgrList = cloudMetaDataRepository.getValues("Client Manager");
					projectEntryLovPo.setClientManager(clientMgrList);
				} else if (lovValues[i].equalsIgnoreCase("KPI_AGGREGATION_LEVEL")) {
					List<LovValuesPo> kpiAggLevelList = cloudMetaDataRepository.getValues("KPI Aggregation Level");
					projectEntryLovPo.setKpiAggreagationLevel(kpiAggLevelList);
				} else if (lovValues[i].equalsIgnoreCase("PROJECT_STATUS")) {
					List<LovValuesPo> projectStatusList = cloudMetaDataRepository.getValues("Project Status");
					projectEntryLovPo.setProjectStatus(projectStatusList);
				} else if (lovValues[i].equalsIgnoreCase("ACCESS_LEVEL")) {
					List<LovValuesPo> accessLevelList = cloudMetaDataRepository.getValues("Access Level");
					projectEntryLovPo.setAccessLevel(accessLevelList);
				} else if (lovValues[i].equalsIgnoreCase("ITERATION_TYPE")) {
					List<LovValuesPo> iterationTypeList = cloudMetaDataRepository.getValues("Iteration Type");
					projectEntryLovPo.setIterationType(iterationTypeList);
				} else if (lovValues[i].equalsIgnoreCase("PROJECT_TYPE")) {
					List<LovValuesPo> projectTypeList = cloudMetaDataRepository.getValues("Project Type");
					projectEntryLovPo.setProjectType(projectTypeList);
				} else if (lovValues[i].equalsIgnoreCase("CLIENT_PROJECT_NUM")) {
					List<LovValuesPo> clientprojectNoList = cloudMetaDataRepository.getValues("Client Project Number");
					projectEntryLovPo.setClientProjectNumber(clientprojectNoList);
				} else if (lovValues[i].equalsIgnoreCase("TASK_TYPE")) {
					List<LovValuesPo> taskTypeList = cloudMetaDataRepository.getValues("Task Type");
					projectEntryLovPo.setTaskType(taskTypeList);
				} else if (lovValues[i].equalsIgnoreCase("RELATED_TO")) {
					List<LovValuesPo> realtedToList = cloudMetaDataRepository.getValues("Related To");
					projectEntryLovPo.setRelatedTo(realtedToList);
				}
			}
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		return projectEntryLovPo;
	}

	@Override
	public List<Object> getPods() throws Exception {
		log.info("Start of getPods in service ######");
		List<Object> podValueList = new ArrayList<>();

		try {
			podValueList = xxrLookUpValuesRepository.getPods();
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		return podValueList;
	}

	@Override
	@SuppressWarnings("rawtypes")
	public CommonLovPo getCommonLov(Long podId) throws Exception {
		log.info("Start of getCommonLov in service ######");

		CommonLovPo commonLovPo = new CommonLovPo();
		List<LovValuesPo> projectsLov = new ArrayList<>();
		List parentObjectLov = new ArrayList<>();
		List<ObjectCodeLovPo> objectCodeLovPo = new ArrayList<>();
		List<ProjectsLovPo> projectLovList = new ArrayList<>();

		// List<BuLovPo> buLovList=new ArrayList<>();

		// ObjectCodeLovPo objectCodeLovpo=new ObjectCodeLovPo();

		try {
			projectsLov = xxrLookUpValuesRepository.getProjectsLov(podId);
			commonLovPo.setPodId(podId);
			String podName = xxrLookUpValuesRepository.getValueById(podId);
			commonLovPo.setPodName(podName);

			for (LovValuesPo project : projectsLov) {
				List<ParentObjectLovPo> parentObjectLovList = new ArrayList<>();
				ProjectsLovPo projectLovPo = new ProjectsLovPo();
				projectLovPo.setProjectId(project.getId());
				projectLovPo.setProjectName(project.getValue());
				parentObjectLov = xxrLookUpValuesRepository.getParentObjectLov(project.getId());
				for (Object parentObject : parentObjectLov) {

					Object[] objects = (Object[]) parentObject;
					BigDecimal parentId = (BigDecimal) objects[0];
					ParentObjectLovPo parentObjectLovPo = new ParentObjectLovPo();
					parentObjectLovPo.setParentObjectId(parentId.longValue());
					parentObjectLovPo.setParentObjectCode((String) objects[1]);
					objectCodeLovPo = xxrLookUpValuesRepository.getObjectLov((String) objects[1]);
					// buLovList=xxrLookUpValuesRepository.getBuLov((String) objects[1]);

					parentObjectLovPo.setObjectCodes(objectCodeLovPo);
					// parentObjectLovPo.setBu(buLovList);
					parentObjectLovList.add(parentObjectLovPo);
				}
				projectLovPo.setParentObjects(parentObjectLovList);
				projectLovList.add(projectLovPo);
			}

			commonLovPo.setProjects(projectLovList);

		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}

		return commonLovPo;
	}

	@Override
	public List<LovValuesPo> getRelatedToLov(String lovValue) throws Exception {
		log.info("Start of getRelatedToLov ######");
		List<LovValuesPo> realtedToList = new ArrayList<>();
		try {
			realtedToList = cloudMetaDataRepository.getValues(lovValue);
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
		return realtedToList;
	}

	@Override
	public List<LovValuesPo> getActualValueLov(String relatedTo) throws Exception {
		log.info("Start of getActualValueLov ######");
		List<LovValuesPo> actualValueList = new ArrayList<>();

		try {

			actualValueList = cloudMetaDataRepository.getValues(relatedTo);

		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
		return actualValueList;
	}

	@Override
	public List<BuLovPo> getBuLov() throws Exception {
		log.info("Start of getBuLov in service ######");
		List<BuLovPo> buLovList = new ArrayList<>();
		try {
			buLovList = xxrLookUpValuesRepository.getBuLov();
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
		return buLovList;
	}

	@Override
	public List<LovValuesPo> getLovByLookUpSetName(String lookupSetName) throws Exception {
		log.info("Start of getLovByLookUpSetName ######");
		List<LovValuesPo> lovValueList = new ArrayList<>();
		try {
		//	lovValueList = cloudMetaDataRepository.getValues(lookupSetName);
			lovValueList =crLookUpValuesRepo.getLookUpValues(lookupSetName);
		} catch (Exception e) {
			 e.printStackTrace();
			throw new Exception(e.getMessage());
		}
		return lovValueList;
	}

	@Override
	public List<ObjectCodeLovPo> getObjectCodes(Long parentObjectId) throws Exception {
		log.info("Start of getObjectCodes in service ######");
		List<ObjectCodeLovPo> objectCodeList = new ArrayList<>();
		String parentObjectCode = "";
		try {

			parentObjectCode = xxrLookUpValuesRepository.getValueById(parentObjectId);
			if (!Validations.isNullOrEmpty(parentObjectCode))
				objectCodeList = xxrLookUpValuesRepository.getObjectLov(parentObjectCode);
		} catch (Exception e) {
			// e.printStackTrace();
			throw new Exception(e.getMessage());
		}
		return objectCodeList;
	}

	@Override
	public List<ObjectCodeLovPo> getAllObjectCodes() throws Exception {
		log.info("Start of getAllObjectCodes in service##");
		List<ObjectCodeLovPo> objectCodeList = new ArrayList<>();
		try {
			objectCodeList = xxrLookUpValuesRepository.getAllObjectLov();
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return objectCodeList;
	}

	@Override
	public List<ParentObjectCodeResPo> getParentObjectCodeByRole(String role, Long podId, Long projectId)
			throws Exception {
		
		log.info("Start of getParentObjectCodeByRole######");
		List<ParentObjectCodeResPo> parentObjectCodeLi = new ArrayList<>();
		//List<XxrRoleObjectLinks> roleObjectLi = new ArrayList<>();
		try {
			// long roleId=xxrRolesRepository.getRoleId(role);
			// roleObjectLi=xxrRoleObjectLinksRepository.findByRoleId(roleId);
			parentObjectCodeLi = xxrRoleObjectLinksRepository.getByrole(role, projectId);
			/*
			 * roleObjectLi.stream().forEach(x -> { ParentObjectCodeResPo parentObject = new
			 * ParentObjectCodeResPo();
			 * parentObject.setParentObjectId(x.getParentObjectId());
			 * parentObject.setParentObjectCode(xxrLookUpValuesRepository.getValueById(x.
			 * getParentObjectId())); parentObjectCodeLi.add(parentObject); });
			 */
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new Exception(e.getMessage());
		}
		return parentObjectCodeLi;
	}

	@Override
	public List<ParentObjectCodeResPo> getParentObjectCodeByRole(String role, Long projectId) throws Exception {
		log.info("Start of getParentObjectCodeByRole######");
		List<ParentObjectCodeResPo> parentObjectCodeLi = new ArrayList<>();
		//List<XxrRoleObjectLinks> roleObjectLi = new ArrayList<>();
		try {
			parentObjectCodeLi = xxrRoleObjectLinksRepository.getByrole(role, projectId);
			
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new Exception(e.getMessage());
		}
		return parentObjectCodeLi;
	}

	@Override
	public List<PodsPo> getPodsByRole(String role) throws Exception {
		log.info("Start of getPodsByRole######");
		List<PodsPo> podList = new ArrayList<>();
		List<PodsPo> podListRes = new ArrayList<>();

		// List<XxrRoleObjectLinks> roleObjectLi = new ArrayList<>();
		try {
			//podList = xxrRoleObjectLinksRepository.getPodsByRole(role);
			/*
			 * roleObjectLi.stream().forEach(x -> { PodsPo podsPo = new PodsPo();
			 * podsPo.setPodId(x.getPodId());
			 * podsPo.setPodName(xxrLookUpValuesRepository.getValueById(x.getPodId()));
			 * podList.add(podsPo); });
			 */
			podListRes = podList.stream().filter(Utils.distinctByKey(PodsPo::getPodId)).collect(Collectors.toList());
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return podListRes;
	}

	@Override
	public List<ProjectsPo> getProjectsByRole(String role, Long podId) throws Exception {
		log.info("Start of getProjectsByRole #######");
		List<ProjectsPo> projectsLi = new ArrayList<>();
		List<ProjectsPo> projectsResLi = new ArrayList<>();
		//List<XxrRoleObjectLinks> roleObjectLi = new ArrayList<>();
		try {
			//projectsLi = xxrRoleObjectLinksRepository.getProjectsByRole(role, podId);
			/*
			 * roleObjectLi.stream().forEach(x -> { ProjectsPo projectPo = new
			 * ProjectsPo(x.getProjectId(),
			 * xxrLookUpValuesRepository.getValueById(x.getProjectId()));
			 * projectsLi.add(projectPo); });
			 */
			projectsResLi = projectsLi.stream().filter(Utils.distinctByKey(ProjectsPo::getProjectId))
					.collect(Collectors.toList());
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return projectsResLi;
	}
	
	@Override
	public List<ProjectsPo> getProjectsByRole(String role) throws Exception {
		log.info("Start of getProjectsByRole #######");
		List<ProjectsPo> projectsLi = new ArrayList<>();
		List<ProjectsPo> projectsResLi = new ArrayList<>();
		//List<XxrRoleObjectLinks> roleObjectLi = new ArrayList<>();
		try {
			projectsLi = xxrRoleObjectLinksRepository.getProjectsByRole(role);
			/*
			 * roleObjectLi.stream().forEach(x -> { ProjectsPo projectPo = new
			 * ProjectsPo(x.getProjectId(),
			 * xxrLookUpValuesRepository.getValueById(x.getProjectId()));
			 * projectsLi.add(projectPo); });
			 */
			projectsResLi = projectsLi.stream().filter(Utils.distinctByKey(ProjectsPo::getProjectId))
					.collect(Collectors.toList());
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return projectsResLi;
	}

	@Override
	public List<ParentObjectCodeResPo> getParentObjectCodes(String role) throws Exception {
		log.info("Start of getParentObjectCodes######");
		List<ParentObjectCodeResPo> parentObjectCodeLi = new ArrayList<>();
		//List<XxrRoleObjectLinks> roleObjectLi = new ArrayList<>();
		try {
			// long roleId=xxrRolesRepository.getRoleId(role);
			// roleObjectLi=xxrRoleObjectLinksRepository.findByRoleId(roleId);
			parentObjectCodeLi = xxrRoleObjectLinksRepository.getParentObjectCodeByrole(role);
			parentObjectCodeLi = parentObjectCodeLi.stream()
					.filter(Utils.distinctByKey(ParentObjectCodeResPo::getParentObjectId)).collect(Collectors.toList());
			/*
			 * roleObjectLi.stream().forEach(x -> { ParentObjectCodeResPo parentObject = new
			 * ParentObjectCodeResPo();
			 * parentObject.setParentObjectId(x.getParentObjectId());
			 * parentObject.setParentObjectCode(xxrLookUpValuesRepository.getValueById(x.
			 * getParentObjectId())); parentObjectCodeLi.add(parentObject); });
			 */
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new Exception(e.getMessage());
		}
		return parentObjectCodeLi;
	}

}
