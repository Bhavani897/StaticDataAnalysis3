package com.rite.products.convertrite.service;

import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.po.CrCloudMetadataPo;
import com.rite.products.convertrite.po.CrCloudTemplateColumnsResPo;
import com.rite.products.convertrite.po.LoadMetaDataFromCloudRes;
import com.rite.products.convertrite.po.SaveCloudTemplateColumnsResPo;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

public interface CrMetadataService {

    LoadMetaDataFromCloudRes loadMetaDataFromCloud(CrCloudMetadataPo crCloudMetadataPo, HttpServletRequest request) throws ValidationException, Exception;

    List<CrCloudTemplateColumnsResPo> generateSequence(Long templateId, Long objectId, String version, HttpServletRequest request) throws Exception;

    LoadMetaDataFromCloudRes loadHdlCloudMetaData(Long objectId, String metaDataTableName, MultipartFile file,String objectCode) throws Exception;

}
