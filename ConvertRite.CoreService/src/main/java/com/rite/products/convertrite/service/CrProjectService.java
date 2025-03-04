package com.rite.products.convertrite.service;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.rite.products.convertrite.model.CrProjectActivities;
import com.rite.products.convertrite.model.CrProjects;
import com.rite.products.convertrite.po.*;
import org.springframework.http.HttpHeaders;

import com.rite.products.convertrite.exception.ValidationException;

public interface CrProjectService {


    SaveProjectHeaderResponsePo saveProjectHeaders(CrProjects projectHeadersPo, HttpServletRequest request) throws Exception;

    List<CrProjects> getAllProjectHeaders() throws Exception;

    List<CrProjectActivities> getProjectLinesById(ProjectLinesReqPo projectLinesReqPo, HttpHeaders httpHeaders) throws Exception;


    ActivitiesResPo upsertProjectActivities(List<CrProjectActivities> activitiesPo, HttpServletRequest request) throws Exception;


    Object getObjectsByUserId(Long userId, String bearerToken);


    Object
    getProjectsAndObjects(Long clientId, Long podId, String projectCode, HttpServletRequest request) throws JsonProcessingException;

    Object
    updateProjectObjects(Long clientId, Long podId, Long projectId, HttpServletRequest request) throws JsonProcessingException;

    Object insertProjectActivities(Long projectId);

    Object getParentObjects(Long projectId, String conversionType);

    Object getObjectsByObjectCode(Long projectId, String objectCode);

    Boolean getPreCloudSetupStatusEnable();
}
