package com.rite.products.convertrite.service;

import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.web.multipart.MultipartFile;

import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.XxrSrcSystemSqlClob;
import com.rite.products.convertrite.po.DbLinkReqPo;
import com.rite.products.convertrite.po.DbLinkResPo;
import com.rite.products.convertrite.po.LoadDataFromEbsReqPo;
import com.rite.products.convertrite.po.LoadDataFromEbsResPo;
import com.rite.products.convertrite.po.LoadMetaDataFromEbsReqPo;
import com.rite.products.convertrite.po.LoadMetaDataFromEbsRes;
import com.rite.products.convertrite.po.SaveEbsIntegrationDetailsPo;
import com.rite.products.convertrite.po.SaveEbsIntegrationResponsePo;
import com.rite.products.convertrite.po.SaveEbsViewReqpo;
import com.rite.products.convertrite.po.SrcSystemLobIdRes;
import com.rite.products.convertrite.po.XxrEbsIntegrationDetailsResPo;

public interface EbsIntegrationService {

	SaveEbsIntegrationResponsePo saveEbsIntegrationDetails(SaveEbsIntegrationDetailsPo ebsIntegartionDetailsPo) throws Exception;

	List<XxrEbsIntegrationDetailsResPo> getEbsIntegrationDetails() throws Exception;

	LoadDataFromEbsResPo loadDataFromEbs(LoadDataFromEbsReqPo loadDataFromEbsReqPo,String userId)throws Exception;

	DbLinkResPo getDbLink(DbLinkReqPo dbLinkReqPo)throws Exception;

	LoadMetaDataFromEbsRes loadMetaDataFromEbs(LoadMetaDataFromEbsReqPo loadMetaDataFromEbsReqPo)throws Exception;

	XxrSrcSystemSqlClob uploadSqlFile(MultipartFile file, String sourceSystem, String objectCode, String version)throws Exception;

	LoadDataFromEbsResPo loadDataFromEbsV1(LoadDataFromEbsReqPo loadDataFromEbsReqPo, HttpServletRequest request) throws Exception;
	void downloadEbsView(Long id, HttpServletResponse response) throws ValidationException, Exception;

	SrcSystemLobIdRes getSrcSystemLobId(String objectCode, String sourceSystem, String version);

	XxrSrcSystemSqlClob saveEbsView(SaveEbsViewReqpo saveEbsViewReqpo)throws Exception;

}
