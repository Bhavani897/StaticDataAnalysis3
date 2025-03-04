package com.rite.products.convertrite.service;

import com.rite.products.convertrite.respository.CloudTemplateHeaderDaoImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import javax.servlet.http.HttpServletResponse;

@Service
public class CrReconcileService {

    @Autowired
    CloudTemplateHeaderDaoImpl cloudTemplateHeaderDaoImpl;
    public Object downloadCloudImportSuccessRec(Long cloudTemplateId, String batchName,HttpServletResponse response) throws Exception {
        return cloudTemplateHeaderDaoImpl.downloadCloudImportSuccessRec(cloudTemplateId, batchName,response);
    }

    public Object downloadCloudImportRejRec(Long cloudTemplateId, String batchName, String userId, HttpServletResponse response) throws Exception {
        return cloudTemplateHeaderDaoImpl.downloadCloudImportRejRec(cloudTemplateId, batchName,userId,response);
    }

    public Object downloadValFailRec(Long cloudTemplateId, String batchName, HttpServletResponse response) throws Exception {

        Object res=null;
        try {
            res = cloudTemplateHeaderDaoImpl.downloadCloudImportFailRec(cloudTemplateId, batchName,response);
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return res;

    }
}
