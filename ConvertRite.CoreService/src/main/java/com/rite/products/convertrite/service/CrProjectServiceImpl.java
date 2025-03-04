package com.rite.products.convertrite.service;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rite.products.convertrite.model.CrPreLoadCloudSetUpsReqPo;
import com.rite.products.convertrite.model.CrProjectActivities;
import com.rite.products.convertrite.model.CrProjects;
import com.rite.products.convertrite.model.CrProjectsObjects;
import com.rite.products.convertrite.po.*;
import com.rite.products.convertrite.respository.*;
import com.rite.products.convertrite.utils.LookUpSets;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.*;
import org.springframework.stereotype.Service;

import com.rite.products.convertrite.exception.BadRequestException;
import org.springframework.web.client.RestTemplate;

@Slf4j
@Service
public class CrProjectServiceImpl implements CrProjectService {

    @Value("${convertrite-admin-host}")
    String ConvertriteAdminHost;
    @Value("${preload-cldsetups-enable}")
    boolean preLoadCldSetupsEnable;
    @Value("${preload-cloud-setups-url}")
    String preLoadCloudSetupsUrl;
    @Value("${process-validation-objects-url}")
    String processValidationObjectsUrl;
    @Autowired
    ProjectEntryDaoImpl projectEntryDaoImpl;
    @Autowired
    XxrProjectWbsTabRepository xxrProjectWbsTabRepository;
    @Autowired
    XxrProjectsRepository xxrProjectsRepository;
    @Autowired
    CrProjectsRepo crProjectsRepo;
    @Autowired
    CrProjectsObjectsRepo crProjectsObjectsRepo;
    @Autowired
    XxrActivitiesRepository xxrActivitiesRepository;

    @Autowired
    CrProjectActivitiesRepo crProjectActivitiesRepo;
    @Autowired
    XxrLookUpValuesRepository xxrLookUpValuesRepository;
    @Autowired
    SaveProjectHeaderDaoImpl saveProjectHeaderDaoImpl;
    @Autowired
    SaveProjectActivitiesDaoImpl saveProjectActivitiesDaoImpl;
    @Autowired
    XxrRoleObjectLinksRepository xxrRoleObjectLinksRepository;
    @Autowired
    CrPreLoadCloudTemplatesService crPreLoadCloudTemplatesService;

    @Autowired
    RestTemplate restTemplate;

    @Override
    public SaveProjectHeaderResponsePo saveProjectHeaders(CrProjects projectHeadersPo,
                                                          HttpServletRequest request) throws Exception {
        log.info("Start of saveProjectHeaders Method in Service ####");
        SaveProjectHeaderResponsePo saveProjectHeaderResponsePo = new SaveProjectHeaderResponsePo();
        Long projectId = null;
        String message = "";
        try {
            //message = saveProjectHeaderDaoImpl.saveProjectHeader(projectHeadersPo, request);
//			CrProjects project=crProjectsRepo.findByProjectId(projectHeadersPo.getProjectId());
//			if(project==null){
            log.info("insert===>");
            saveProjectHeaderResponsePo = saveOrUpdateProjectHeaders(projectHeadersPo);
//			}else{
//				log.info("update===>");
//				project.setProjectName(projectHeadersPo.getProjectName());
//				project.setProjectStatus(projectHeadersPo.getProjectStatus());
//				project.setDescription(projectHeadersPo.getDescription());
//				saveProjectHeaderResponsePo=	saveOrUpdateProjectHeaders(project);
//			}

            //			List list=new ArrayList();
//			list.add(projectHeadersPo);
//			int userId= Integer.parseInt(request.getHeader("userId"));
//			message = xxrProjectsRepository.saveProjectHeaders(list, userId);

//			saveProjectHeaderResponsePo.setMessage(message);

//			if (!Validations.isNullOrEmpty(projectHeadersPo.getProjectName()) ) {
//				projectId = xxrProjectsRepository.getProjectId(projectHeadersPo.getProjectName(),
//						projectHeadersPo.getPodId());
//				saveProjectHeaderResponsePo.setProjectId(projectId);
            saveProjectHeaderResponsePo.setProjectName(projectHeadersPo.getProjectName());
//			}
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return saveProjectHeaderResponsePo;
    }

    private SaveProjectHeaderResponsePo saveOrUpdateProjectHeaders(CrProjects projectHeadersPo) {
        String message = null;
        SaveProjectHeaderResponsePo saveProjectHeaderResponsePo = new SaveProjectHeaderResponsePo();
        try {
            CrProjects pro = crProjectsRepo.save(projectHeadersPo);
            if (pro != null) {
                message = "Project Header successfully saved";
                saveProjectHeaderResponsePo.setMessage(message);
                saveProjectHeaderResponsePo.setProjectId(pro.getProjectId());
            }
        } catch (Exception e) {
            message = "Error  while saving Project Header";
            saveProjectHeaderResponsePo.setMessage(message);
            saveProjectHeaderResponsePo.setError(e.getMessage());
            e.printStackTrace();

        }
        return saveProjectHeaderResponsePo;
    }

    private List<CrProjectActivities> saveOrUpdateProjectActivities(List<CrProjectActivities> activitiesPo) {
        List<CrProjectActivities> res = null;
        try {
            res = crProjectActivitiesRepo.saveAll(activitiesPo);

        } catch (Exception e) {
            //resMsg = "Exception while saving Project Activities";
            e.printStackTrace();
            log.error("Exception in saveOrUpdateProjectActivities--> {} ", e.getMessage(), e);
        }
        return res;
    }

    @Override
    public List<CrProjects> getAllProjectHeaders() throws Exception {
        log.info("Start of getAllProjectHeaders Method in Service ####");
        List<CrProjects> xxrProjectsResPoList = new ArrayList<>();
        try {
            xxrProjectsResPoList = crProjectsRepo.findAll();
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return xxrProjectsResPoList;
    }

    @Override
    public ActivitiesResPo upsertProjectActivities(List<CrProjectActivities> activitiesPo, HttpServletRequest request)
            throws BadRequestException, Exception {
        log.info("Start of upsertProjectActivities Method in Service ####");
        ActivitiesResPo activitiesResPo = new ActivitiesResPo();
        String msg = "";
        try {
            List<CrProjectActivities> res = saveOrUpdateProjectActivities(activitiesPo);
            activitiesResPo.setMessage(msg);
            if (res.size() > 0) {
                activitiesResPo.setCrActivities(res);
            } else {
               log.info("No data Found--.>");
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(e.getMessage());
        }
        return activitiesResPo;
    }

    @Override
    public List<CrProjectActivities> getProjectLinesById(ProjectLinesReqPo projectLinesReqPo, HttpHeaders httpHeaders) throws Exception {
        log.info("Start of getProjectLinesById#######");
        List<CrProjectActivities> projActivitiesLi = new ArrayList<>();
        Pageable pageable = PageRequest.of(projectLinesReqPo.getPageNo(),
                projectLinesReqPo.getPageSize(),
                Sort.by(projectLinesReqPo.getSortDirection(), projectLinesReqPo.getSortBy()));

        Page<CrProjectActivities> pageContent = crProjectActivitiesRepo.findAllByProjectId(projectLinesReqPo.getProjectId(), pageable);

        httpHeaders.set("pagecount", String.valueOf(pageContent.getTotalPages()));
        httpHeaders.set("totalcount", String.valueOf(pageContent.getTotalElements()));
        if (pageContent.hasContent()) {
            projActivitiesLi = pageContent.getContent();
        }
        log.info(" {} ::::::count", projActivitiesLi.size());
        return projActivitiesLi;
    }

    @Override
    public Object getProjectsAndObjects(Long clientId, Long podId, String projectCode, HttpServletRequest request) {
        HttpHeaders header = new HttpHeaders();
        header.setAccept(Arrays.asList(MediaType.APPLICATION_JSON));
        log.info("bearerToken--> {} ", request.getHeader("Authorization"));
        header.set("Authorization", request.getHeader("Authorization"));
        HttpEntity<String> entity = new HttpEntity<String>(header);
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = null;
        JsonNode name;
        List<ProjectsWithObjectsPo> pojos = null;
        List<ProjectsWithObjectsPo> newJsonNode = null;
        try {
            log.info("ConvertriteAdminHost----> {} ", ConvertriteAdminHost);
            ResponseEntity<String> projectsPo = restTemplate.exchange(ConvertriteAdminHost + "/api/convertriteadmin/getProjectsAndObjects?clientId=" + clientId + "&podId=" + podId + "&projectName=" + projectCode, HttpMethod.GET, entity, String.class);

            root = mapper.readTree(projectsPo.getBody());
            name = root.path("payload");

            newJsonNode = mapper.treeToValue(name, List.class);
            if (newJsonNode.size() > 0) {
                pojos = mapper.convertValue(name, new TypeReference<List<ProjectsWithObjectsPo>>() {
                });

            } else {
                return "No data Found";
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
        List<CrProjects> projList = new ArrayList<CrProjects>();
        try {
            Long projectId=null;
            List<ObjectsPo> objectPoLi=new ArrayList<>();
            List<CrProjectsObjects> crProjectsObjectsList = new ArrayList<>();
            for (ProjectsWithObjectsPo p : pojos) {
                projectId=p.getProjectId();
                CrProjects proj = new CrProjects();
                proj.setProjectId(projectId);
                proj.setProjectName(p.getProjectName());
                proj.setProjectCode(p.getProjectCode());
                proj.setStartDate(new java.sql.Date(new java.util.Date().getTime()));
                proj.setLastUpdatedBy("ConvertRite");
                proj.setLastUpdateDate(new java.sql.Date(new java.util.Date().getTime()));
                int count = 0;
                objectPoLi = mapper.convertValue(p.getObjects(), new TypeReference<List<ObjectsPo>>() {
                });

                for (ObjectsPo objPo : objectPoLi) {
                    CrProjectsObjects crProjectsObjects = new CrProjectsObjects();
                    crProjectsObjects.setProjectObjLinkId(count++);
                    crProjectsObjects.setProjectId(p.getProjectId());
                    crProjectsObjects.setProjectCode(p.getProjectCode());
                    crProjectsObjects.setObjectId(objPo.getObjectId());
                    crProjectsObjects.setObjectCode(objPo.getObjectCode());
                    crProjectsObjects.setObjectName(objPo.getObjectName());
                    crProjectsObjects.setModuleCode(objPo.getModuleCode());
                    crProjectsObjects.setConversionType(objPo.getConversionType());
                    crProjectsObjects.setCreationDate(new Date());
                    crProjectsObjects.setCreatedBy("ConvertRite");
                    if (!Objects.isNull(objPo.parentObjectId)) {
                        for (ObjectsPo innerobjPo : objectPoLi) {
                            if (innerobjPo.objectId.equals(objPo.parentObjectId)) {
                                crProjectsObjects.setParentObjectCode(innerobjPo.objectCode);
                                break;
                            }
                        }
                    }
                    crProjectsObjectsList.add(crProjectsObjects);
                }
                crProjectsObjectsRepo.saveAll(crProjectsObjectsList);
                projList.add(proj);
            }
            crProjectsRepo.saveAll(projList);
            //preload cloud setups will be loaded only when flag is enabled
            if (preLoadCldSetupsEnable) {
                intitatePreLoadCldSetUpsAndValidationApi(objectPoLi,projectId,request);
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
        return projList;
    }

    private void intitatePreLoadCldSetUpsAndValidationApi(List<ObjectsPo> objectPoLi,Long projectId,HttpServletRequest request){
        log.info("intitatePreLoadCldSetUpsAndValidationApi -> objectSize {}",objectPoLi.size());
        try {
            List<Long> objectLi = objectPoLi.stream().map(x -> x.getObjectId().longValue()).collect(Collectors.toList());
            //async Post API call for create cloud setups
            initiateCloudSetupsCreation(objectLi, projectId, request);
            //async API call for processing validation tables
            initiateProcessValidationTables(objectLi, projectId, request);
        }catch(Exception e){
            log.error("Error in intitatePreLoadCldSetUpsAndValidationApi -> {} ", e.getMessage());
        }
    }
    @Override
    public Object updateProjectObjects(Long clientId, Long podId, Long projectId,HttpServletRequest request) {
        CrProjects existingProject = crProjectsRepo.findByProjectId(projectId);
        List<CrProjectsObjects>  oldProjectObjects=crProjectsObjectsRepo.findAllByProjectId(projectId);
        HttpHeaders header = new HttpHeaders();
        header.setAccept(Arrays.asList(MediaType.APPLICATION_JSON));
        log.info("In updateProjectObjects(), bearerToken--> {} ", request.getHeader("Authorization"));
        header.set("Authorization", request.getHeader("Authorization"));
        HttpEntity<String> entity = new HttpEntity<String>(header);
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = null;
        JsonNode name;
        List<ProjectsWithObjectsPo> pojos = null;
        List<ProjectsWithObjectsPo> newJsonNode = null;
        try {
            log.info("In updateProjectObjects(), ConvertriteAdminHost----> {} ", ConvertriteAdminHost);
            ResponseEntity<String> projectsPo = restTemplate.exchange(ConvertriteAdminHost + "/api/convertriteadmin/getProjectsAndObjects?clientId=" + clientId + "&podId=" + podId + "&projectName=" + existingProject.getProjectCode(), HttpMethod.GET, entity, String.class);
            root = mapper.readTree(projectsPo.getBody());name = root.path("payload");
            newJsonNode = mapper.treeToValue(name, List.class);
            if (newJsonNode.size() > 0) {
                pojos = mapper.convertValue(name, new TypeReference<List<ProjectsWithObjectsPo>>() {
                });
            } else {
                return "No data Found";
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        List<CrProjects> projList = new ArrayList<CrProjects>();
        try {
            List<CrProjectsObjects> crProjectsObjectsList = new ArrayList<>();
            for (ProjectsWithObjectsPo p : pojos) {
                int count = 0;
                List<ObjectsPo> objList = mapper.convertValue(p.getObjects(), new TypeReference<List<ObjectsPo>>() {
                });
                log.info("Original ProjectObjectsPo size: {} ", objList.size());
                List<ObjectsPo> newObjects=new ArrayList<>();
                for (ObjectsPo objPo : objList) {
                    boolean newObjectFlag=true;
                    //TO find delta or new objects during re-import of project
                    for(CrProjectsObjects oldParentObject:oldProjectObjects){
                        if(oldParentObject.getObjectId().equals(objPo.getObjectId())){
                            newObjectFlag=false;
                            break;
                        }
                    }
                    CrProjectsObjects crProjectsObjects = new CrProjectsObjects();
                    if(newObjectFlag) {
                        newObjects.add(objPo);
                        crProjectsObjects.setProjectObjLinkId(count++);
                        crProjectsObjects.setProjectId(p.getProjectId());
                        crProjectsObjects.setProjectCode(p.getProjectCode());
                        crProjectsObjects.setObjectId(objPo.getObjectId());
                        crProjectsObjects.setObjectCode(objPo.getObjectCode());
                        crProjectsObjects.setObjectName(objPo.getObjectName());
                        crProjectsObjects.setModuleCode(objPo.getModuleCode());
                        crProjectsObjects.setConversionType(objPo.getConversionType());
                        crProjectsObjects.setCreationDate(new Date());
                        crProjectsObjects.setCreatedBy("ConvertRite");
                        if (!Objects.isNull(objPo.parentObjectId)) {
                            for (ObjectsPo innerobjPo : objList) {
                                if (innerobjPo.objectId.equals(objPo.parentObjectId)) {
                                    crProjectsObjects.setParentObjectCode(innerobjPo.objectCode);
                                    break;
                                }
                            }
                        }
                        crProjectsObjectsList.add(crProjectsObjects);
                    }
                }
                log.info("Additional ProjectObjectsPo size: {}", crProjectsObjectsList.size());
                crProjectsObjectsRepo.saveAll(crProjectsObjectsList);
                projList.add(existingProject);
                //preload cloud setups will be loaded only when flag is enabled
                if (preLoadCldSetupsEnable) {
                    intitatePreLoadCldSetUpsAndValidationApi(newObjects,projectId,request);
                }
            }
        } catch (Exception e) {
            log.error("Error in updateprojects->{}",e);
            throw new RuntimeException(e);
        }
        return projList;
    }

    private void initiateCloudSetupsCreation(List<Long> objectLi,Long projectId,HttpServletRequest request){
        log.info("initiateCloudSetupsCreation->{}",preLoadCloudSetupsUrl);
        CrPreLoadCloudSetUpsReqPo preLoadCloudSetUpsReqPo=new CrPreLoadCloudSetUpsReqPo();
        preLoadCloudSetUpsReqPo.setObjectIdLi(objectLi);
        preLoadCloudSetUpsReqPo.setProjectId(projectId);
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set("Accept", MediaType.APPLICATION_JSON_VALUE);
        httpHeaders.setContentType(MediaType.APPLICATION_JSON);
        httpHeaders.set("X-TENANT-ID", String.valueOf(request.getHeader("X-TENANT-ID")));
        httpHeaders.set("userId", request.getHeader("userId"));
        //Calling RestApi to create cloud templates & staging table
        HttpEntity<CrPreLoadCloudSetUpsReqPo> requestEntity = new HttpEntity<>(preLoadCloudSetUpsReqPo,httpHeaders);
        CompletableFuture.runAsync(() -> {
            ResponseEntity<?> response = restTemplate.exchange(preLoadCloudSetupsUrl, HttpMethod.POST, requestEntity, Void.class);
        });
    }

    private void initiateProcessValidationTables(List<Long> objectLi,Long projectId,HttpServletRequest request){
        log.info("initiateProcessValidationTables-> {} ", processValidationObjectsUrl);
        ProcessValidationObjectsReqPo processValidationObjectsReq=new ProcessValidationObjectsReqPo();
        processValidationObjectsReq.setObjectIdsLi(objectLi);
        processValidationObjectsReq.setProjectId(projectId);
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set("Accept", MediaType.APPLICATION_JSON_VALUE);
        httpHeaders.setContentType(MediaType.APPLICATION_JSON);
        httpHeaders.set("Authorization",request.getHeader("Authorization"));
        httpHeaders.set("X-TENANT-ID", String.valueOf(request.getHeader("X-TENANT-ID")));
        httpHeaders.set("userId", request.getHeader("userId"));
        //Calling RestApi to create process validation tables
        HttpEntity<ProcessValidationObjectsReqPo> requestEntity = new HttpEntity<>(processValidationObjectsReq,httpHeaders);
        CompletableFuture.runAsync(() -> {
            ResponseEntity<?> response = restTemplate.exchange(processValidationObjectsUrl, HttpMethod.POST, requestEntity, Void.class);
        });
    }

    @Override
    public Object insertProjectActivities(Long projectId) {
        List<CrProjectActivities> activitieList = new ArrayList<>();
        List<CrProjectsObjects> crProjectsObjectsList = crProjectsObjectsRepo.findAllByProjectId(projectId);
        List<String> lookUpSetsList = null;
        try {
            LookUpSets lookUpSets = new LookUpSets();
            lookUpSetsList = lookUpSets.getList();
            log.info("Look Ups Size-->" + lookUpSetsList.size());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        int count = 0;

        List<CrProjectActivities> existingActivities = crProjectActivitiesRepo.findAllByProjectId(projectId);
        List<String> existingTasks = existingActivities.stream().map(CrProjectActivities::getTaskName).collect(Collectors.toList());

        count = existingActivities.size();

        for (CrProjectsObjects crProjectsObject : crProjectsObjectsList) {
            for (String task : lookUpSetsList) {
                if(existingTasks.size() == 0 || !existingTasks.contains(crProjectsObject.getObjectName() + "_" + task)) {
                    int currentIndex = count;
                    CrProjectActivities activitie = new CrProjectActivities();
                    activitie.setProjectId(crProjectsObject.getProjectId());
                    activitie.setTaskName(crProjectsObject.getObjectName() + "_" + task);
                    activitie.setObjectId(crProjectsObject.getObjectId());
                    activitie.setSeq(currentIndex);
                    activitie.setTaskNum("" + currentIndex);
                    activitie.setTaskType("Task Type");
                    activitie.setLastUpdatedBy("ConvertRiteAdmin");
                    activitie.setLastUpdateDate(new java.sql.Date(new java.util.Date().getTime()));
                    activitieList.add(activitie);
                    count++;
                }
            }
        }
        if(activitieList.size() > 0) {
            crProjectActivitiesRepo.saveAll(activitieList);
        }
        return crProjectActivitiesRepo.findAll();
    }

    @Override
    public Object getParentObjects(Long projectId, String conversionType) {
        if(conversionType == null || conversionType.isEmpty()) {
            return crProjectsObjectsRepo.getAllByProjectId(projectId);
        }
        else  {
            return crProjectsObjectsRepo.getAllByProjectIdAndConversionTypeIgnoreCaseAndParentObjectCodeIsNull(projectId,conversionType);
        }
    }

    @Override
    public Object getObjectsByObjectCode(Long projectId, String objectCode) {
        return crProjectsObjectsRepo.getAllByObjectCode(projectId, objectCode);
    }

    @Override
    public Object getObjectsByUserId(Long userId, String bearerToken) {
        HttpHeaders header = new HttpHeaders();
        header.setAccept(Arrays.asList(MediaType.APPLICATION_JSON));
        log.info("In getObjectsByUserId(), bearerToken--> {} ", bearerToken);
        header.set("Authorization", bearerToken);
        HttpEntity<String> entity = new HttpEntity<String>(header);
        RestTemplate restTemplate = new RestTemplate();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = null;
        try {
            ResponseEntity<String> projectsPo = restTemplate.exchange(ConvertriteAdminHost + "/api/convertriteadmin/getObjectsByUserId/" + userId, HttpMethod.GET, entity, String.class);
            log.info("projectsPo--> {} ", projectsPo);
            root = mapper.readTree(projectsPo.getBody());
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return root;
    }

    public Boolean getPreCloudSetupStatusEnable(){
        return preLoadCldSetupsEnable;
    }
}
