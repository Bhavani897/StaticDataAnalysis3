package com.rite.products.convertrite.service;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.model.XxrActivities;
import com.rite.products.convertrite.model.XxrUsers;
import com.rite.products.convertrite.po.DashBoardProjDatesPo;
import com.rite.products.convertrite.po.DashBoardResPo;
import com.rite.products.convertrite.po.GantChatResPo;
import com.rite.products.convertrite.respository.XxrActivitiesRepository;
import com.rite.products.convertrite.respository.XxrLookUpValuesRepository;
import com.rite.products.convertrite.respository.XxrProjectsRepository;
import com.rite.products.convertrite.respository.XxrUsersRepository;
@Service
public class DashBoardServiceImpl implements DashBoardService{
	private static final Logger log = LoggerFactory.getLogger(DashBoardServiceImpl.class);
	@Autowired
	private XxrActivitiesRepository xxrActivitiesRepository;
	@Autowired
	private XxrUsersRepository xxrUserRepository;
	@Autowired
	 private XxrLookUpValuesRepository xxrLookUpValuesRepository;
	@Autowired
	private XxrProjectsRepository xxrProjectRepository;

	@Override
	public List<DashBoardResPo> getTask(String user) throws BadRequestException,Exception {
		log.info("getTask method in Dashboard Service###");
		List<XxrActivities> activityLi = new ArrayList<>();
		
		List<DashBoardResPo> dashBoardResLi = new ArrayList<>();
		
		XxrUsers userRec = xxrUserRepository.findByNameIgnoreCase(user);
		long userId;
		if(userRec != null) {
			 userId  = userRec.getId();
		}else {
			throw new BadRequestException(" this user is  not a valid user");
			}
		try {
			activityLi = xxrActivitiesRepository.getActivityByTaskOwnerId(userId);
			activityLi.stream().forEach(x -> {
				DashBoardResPo resPo = new DashBoardResPo();
				String projectName = xxrLookUpValuesRepository.getValueById(x.getProjectId());
				String objectCode = xxrLookUpValuesRepository.getValueById(x.getObjectId());
				String parentObjectCode = xxrLookUpValuesRepository.getValueById(x.getParentObjectId());
				String podName = xxrLookUpValuesRepository.getValueById(x.getPodId());
				
				DashBoardProjDatesPo  projectDatesPo = xxrProjectRepository.getProjDatesById(x.getProjectId(),x.getPodId());
				if(projectDatesPo != null)
					resPo.setProjectStartDate(projectDatesPo.getStartDate());
				if(projectDatesPo != null)
					resPo.setProjectEndDate(projectDatesPo.getEndDate());
				String legacyResource = "";
				String destinationResource = "";
				String cloudResource = "";
				String integratorResource = "";
				String clientResource = "";
				resPo.setParentObjectId(x.getParentObjectId());
				resPo.setProjectId(x.getProjectId());
				resPo.setProjectName(projectName);
				if (x.getTaskId() != null)
					resPo.setTaskId(x.getTaskId());
				resPo.setSeq(x.getSeq());
				resPo.setWbsId(x.getWbsId());
				resPo.setPodId(x.getPodId());
				if (!Validations.isNullOrEmpty(podName))
					resPo.setPodName(podName);
				if (!Validations.isNullOrEmpty(x.getTaskNum()))
					resPo.setTaskNum(x.getTaskNum());
				if (!Validations.isNullOrEmpty(x.getTaskName()))
					resPo.setTaskName(x.getTaskName());
				resPo.setObjectId(x.getObjectId());
				if (!Validations.isNullOrEmpty(objectCode))
					resPo.setObjectCode(objectCode);
				if (!Validations.isNullOrEmpty(parentObjectCode))
					resPo.setParentObjectCode(parentObjectCode);
				if (!Validations.isNullOrEmpty(x.getTaskType()))
					resPo.setTaskType(x.getTaskType());
				if (!Validations.isNullOrEmpty(x.getPreReqTask()))
					resPo.setPreReqTask(x.getPreReqTask());
				if (x.getStartDate() != null)
					resPo.setStartDate(x.getStartDate());
				if (x.getEndDate() != null)
					resPo.setEndDate(x.getEndDate());
				if (x.getWeightage() != null)
					resPo.setWeightage(x.getWeightage());
				resPo.setCompletePercentage(x.getCompletePercentage());
				if (x.getLegacyResourceId() != null) {
					resPo.setLegacyResourceId(x.getLegacyResourceId());
					legacyResource = xxrLookUpValuesRepository.getValueById(x.getLegacyResourceId());
					if (!Validations.isNullOrEmpty(legacyResource))
						resPo.setLegacyResource(legacyResource);
				}
				if (!Validations.isNullOrEmpty(x.getTaskStatus()))
				resPo.setTaskStatus(x.getTaskStatus());
				if (x.getDestinationResourceId() != null) {
					resPo.setDestinationResourceId(x.getDestinationResourceId());
					destinationResource = xxrLookUpValuesRepository.getValueById(x.getDestinationResourceId());
					
				}
				if (!Validations.isNullOrEmpty(destinationResource))
					resPo.setDestinationResource(destinationResource);
				resPo.setTaskOwnerId(x.getTaskOwnerId());
				resPo.setTaskOwner(userRec.getName());
				resPo.setCompletionFlag(x.getCompletionFlag());
				if (x.getCloudResourceId() != null) {
					resPo.setCloudResourceId(x.getCloudResourceId());
					cloudResource = xxrLookUpValuesRepository.getValueById(x.getCloudResourceId());
					
				}
				if (!Validations.isNullOrEmpty(cloudResource))
					resPo.setCloudResource(cloudResource);
				if (x.getIntegratorResourceId() != null) {
					integratorResource = xxrLookUpValuesRepository.getValueById(x.getIntegratorResourceId());
					resPo.setIntegratorResourceId(x.getIntegratorResourceId());
					
				}
				if (!Validations.isNullOrEmpty(integratorResource))
					resPo.setIntegratorResource(integratorResource);

				if (x.getClientResourceId() != null) {
					clientResource = xxrLookUpValuesRepository.getValueById(x.getClientResourceId());
					resPo.setClientResourceId(x.getClientResourceId());

				}
				
				if (!Validations.isNullOrEmpty(clientResource))
					resPo.setClientResource(clientResource);
				dashBoardResLi.add(resPo);
			});

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		
		return dashBoardResLi;
	}


	@Override
	public List<GantChatResPo> getGanttChat(String user) throws BadRequestException,Exception {
		log.info("getChatByUser method in DashBoardService ##");
		List<GantChatResPo> chatResPo = new ArrayList<>();
		XxrUsers userRec = xxrUserRepository.findByNameIgnoreCase(user);

		long userId;
		if (userRec != null) {
			userId = userRec.getId();
		} else {
			throw new BadRequestException(" This user is  not a valid user");
		}
		try {

			chatResPo = xxrLookUpValuesRepository.getGanttChat(userId);

		} catch (Exception e) {
			new BadRequestException(e.getMessage());
		}
		return chatResPo;
	}

}
