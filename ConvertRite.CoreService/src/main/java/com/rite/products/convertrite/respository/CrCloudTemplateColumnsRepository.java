package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrCloudTemplateColumns;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface CrCloudTemplateColumnsRepository extends JpaRepository<CrCloudTemplateColumns,Long> {

    List<CrCloudTemplateColumns> findByTemplateId(@Param("templateId") long templateId);
    CrCloudTemplateColumns findByTemplateIdAndColumnName(@Param("templateId") long templateId,@Param("columnName") String columnName);


}
