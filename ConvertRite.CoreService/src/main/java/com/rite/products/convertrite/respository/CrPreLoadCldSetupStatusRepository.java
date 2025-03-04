package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CRPreloadValidationSetupResPo;
import com.rite.products.convertrite.model.CrPreLoadCldSetupStatus;
import com.rite.products.convertrite.model.CRPreloadCldSetupResPo;
import com.rite.products.convertrite.po.CrTemplateMetaDataJobStatusResPo;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface CrPreLoadCldSetupStatusRepository extends JpaRepository<CrPreLoadCldSetupStatus,Long> {

    

    @Query("select  new  com.rite.products.convertrite.po.CrTemplateMetaDataJobStatusResPo(a,b.objectName) from CrPreLoadCldSetupStatus a, CrProjectsObjects b WHERE a.objectId = b.objectId")
    List<CrTemplateMetaDataJobStatusResPo> getByProjectId(Long projectId);

    CrPreLoadCldSetupStatus findByObjectIdAndProjectId(Long objectId,Long projectId);

    @Query("SELECT new com.rite.products.convertrite.model.CRPreloadCldSetupResPo("
            + "crp.setupId, prj.projectId, prj.projectName, obj.objectId, obj.objectName, crp.cldMetaDataTableName, "
            + "crp.cldTemplateName, crp.cldTemplateCode, crp.cldStagingTableName, "
            + "crp.cldSetUpStatus, crp.cldSetUpErrorMessage) "
            + "FROM CrPreLoadCldSetupStatus crp "
            + "JOIN CrProjects prj ON prj.projectId = crp.projectId "
            + "JOIN CrObjects obj ON obj.objectId = crp.objectId "
            + "WHERE crp.projectId = :projectId "
            + "ORDER BY obj.objectId")
    List<CRPreloadCldSetupResPo> fetchCldSetupStatusDetails(@Param("projectId") Long projectId);

    @Query("SELECT new com.rite.products.convertrite.model.CRPreloadValidationSetupResPo("
            + "crp.setupId, prj.projectId, prj.projectName, obj.objectId, obj.objectName, crp.valSyncTables, "
            + "crp.valPkgExecution, crp.valPkgStatus, crp.valPkgErrorMessage) "
            + "FROM CrPreLoadCldSetupStatus crp "
            + "JOIN CrProjects prj ON prj.projectId = crp.projectId "
            + "JOIN CrObjects obj ON obj.objectId = crp.objectId "
            + "WHERE crp.projectId = :projectId "
            + "ORDER BY obj.objectId")
    List<CRPreloadValidationSetupResPo> fetchValidationSetupStatusDetails(@Param("projectId") Long projectId);


}
