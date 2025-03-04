package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrFbdiCols;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface CrFbdiColsRepo extends JpaRepository<CrFbdiCols,Long> {
    CrFbdiCols findByFbdiTemplateIdAndFbdiColumnId(Long fbdiTemplateId, Long fbdiColumnId);

    List<CrFbdiCols> findByfbdiTemplateId(Long fbdiTemplateId);

    @Query("select  c.databaseColumn,c.sequence from CrFbdiCols c,CrFbdiHdrs d where c.fbdiTemplateId=d.fbdiTemplateId and d.version=:version and d.objectId=:objectId")
    public List<Object> getColumnNameandSequence(@Param("objectId") Long objectId, @Param("version") String version);

}
