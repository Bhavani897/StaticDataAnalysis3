package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrObjects;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface CrObjectsRepository extends JpaRepository<CrObjects,Long> {

    @Query("select parentObjectId from CrObjects where objectId=:objectId")
    public Long getParentObjectIdByObjectId(Long objectId);

    CrObjects findByObjectId(Long objectId);
}
