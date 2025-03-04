package com.rite.products.convertrite.service;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.stereotype.Service;

import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.XxrCldTempHdrsView;
import com.rite.products.convertrite.model.XxrCloudTemplateHeader;
import com.rite.products.convertrite.po.CldSrcTemplateIdsResPo;
import com.rite.products.convertrite.po.CloudMappingSetPo;
import com.rite.products.convertrite.po.CloudSourceColumnsPo;
import com.rite.products.convertrite.po.CloudStagingTablePo;
import com.rite.products.convertrite.po.CloudTablesTemplatesPo;
import com.rite.products.convertrite.po.CloudTemplatePo;
import com.rite.products.convertrite.po.CopyCloudReqPo;
import com.rite.products.convertrite.po.CreateDynamicViewPo;
import com.rite.products.convertrite.po.LovPo;
import com.rite.products.convertrite.po.LovValuesPo;
import com.rite.products.convertrite.po.PodsPo;
import com.rite.products.convertrite.po.SaveCloudTemplateColumnsPo;
import com.rite.products.convertrite.po.SaveCloudTemplateColumnsResPo;
import com.rite.products.convertrite.po.SaveCloudTemplateHeadersPo;
import com.rite.products.convertrite.po.SaveTemplateHeaderResponsePo;
import com.rite.products.convertrite.po.XxrCloudTemplateColumnsResPo;
import com.rite.products.convertrite.po.XxrCloudTemplatePo;

@Service
public interface XxrCloudService {

	XxrCloudTemplatePo getAllCloudData() throws Exception;

	CloudSourceColumnsPo getCloudSourceColumns(String sourceTemplateName, String cloudTableName) throws Exception;

	List<XxrCloudTemplateHeader> getCloudTemplate(CloudTemplatePo cloudTemplatePo) throws Exception;

	List<XxrCldTempHdrsView> getAllCloudTemplates(Long roleId) throws Exception;

	List<XxrCloudTemplateColumnsResPo> getCloudTemplateColumns(long templateId) throws Exception;

	LovPo getCloudLovValues(String[] lovValues, long podId, long projectId) throws Exception;

	LovPo getAllLovValues(String[] lovValues) throws Exception;

	//CloudSourceColumnsPo getCloudSourceColumnsByIds(Long templateId, Long tableId) throws Exception;

	List<PodsPo> getPods() throws Exception;

	List<LovValuesPo> getProjectsByPod(long podId) throws Exception;

	List<LovValuesPo> getProjects() throws Exception;

	CloudStagingTablePo createCloudStaggingTab(String tableName, long templateId,HttpServletRequest request) throws Exception;

	CreateDynamicViewPo createDynamicView(long templateId, String stgTableName,HttpServletRequest request) throws Exception;

	//CloudTablesTemplatesPo getCloudTablesTemplates(long projectId, long podId, long objectId,
	//		long parentObjectId) throws Exception;

	SaveTemplateHeaderResponsePo saveCloudHeaders(List<SaveCloudTemplateHeadersPo> sourceTemplateHeadersPo,HttpServletRequest  request)
			throws Exception;

	SaveCloudTemplateColumnsResPo saveCloudTemplateColumns(List<SaveCloudTemplateColumnsPo> cloudTemplateColumnsPo,HttpServletRequest request)
			throws BadRequestException,Exception;

	//List<CloudMappingSetPo> getCloudMappingSetNames(Long projectId, Long podId, Long objectId, Integer bu) throws Exception;
	List<CloudMappingSetPo> getCloudMappingSetNames() throws Exception;

	SaveTemplateHeaderResponsePo copyCloudTemplate(CopyCloudReqPo copyCloudReqPo,HttpServletRequest request) throws Exception;

	SaveCloudTemplateColumnsResPo generateSequence(Long templateId, Long objectId,String version,HttpServletRequest request)throws Exception;

	SaveCloudTemplateColumnsResPo xlsmGenerateSequence(Long templateId, Long objectId, String version,HttpServletRequest request) throws Exception,ValidationException;

	List<CldSrcTemplateIdsResPo> getCldSrcTemplateIds(String objectCode, String parentObjectCode, Long roleId);
	public SaveCloudTemplateColumnsResPo saveCloudTemplateColumnsWithJpa(
			List<SaveCloudTemplateColumnsPo> cloudTemplateColumnsPo) throws BadRequestException,ValidationException;
	SaveTemplateHeaderResponsePo saveCloudTemplateHdrsWithJpa(SaveCloudTemplateHeadersPo saveCloudTemplateHeadersPo)
			throws ValidationException;

	List<CldSrcTemplateIdsResPo> getDefaultCldSrcTemplateIds(String objectCode, String parentObjectCode, Long roleId);

}
