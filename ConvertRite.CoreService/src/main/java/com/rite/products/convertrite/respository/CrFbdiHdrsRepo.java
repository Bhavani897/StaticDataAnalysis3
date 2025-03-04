package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrFbdiHdrs;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface CrFbdiHdrsRepo extends JpaRepository<CrFbdiHdrs,Long> {
    @Query("Select c.fbdiTemplateId from CrFbdiHdrs c where lower(c.fbdiTemplateName)=lower(:fbdiTemplateName) and c.version=:version and c.sheetName=:sheetName and c.objectId=:objectId")
    public Long getFbdiTemplateId(String fbdiTemplateName,String version,String sheetName,Long objectId);

    CrFbdiHdrs findByObjectIdAndVersion(Long objectId, String version);
}
