package com.rite.products.convertrite.respository;

import java.util.List;

import com.rite.products.convertrite.model.CrProjectsObjects;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import com.rite.products.convertrite.model.CrObjectGroupLines;
import com.rite.products.convertrite.model.CrObjectGroupLinesView;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

public interface CrObjectGroupLinesRepo extends JpaRepository<CrObjectGroupLines, Long> {

    @Query("SELECT C FROM CrObjectGroupLines C WHERE C.groupId = :groupId")
    public List<CrObjectGroupLines> getobjectLinesByGroupId(Long groupId);

    @Query("Delete  from CrObjectGroupLines C where C.groupId = :groupId")
    public void deleteAllLinesByGroupId(Long groupId);

    @Query("SELECT C FROM CrObjectGroupLinesView C WHERE C.groupId = :groupId")
    public List<CrObjectGroupLinesView> getObjectLinesViewByGroupId(Long groupId);

    @Query("select groupId from  CrObjectGroupLines where  objectId=:objectId")
    Long getGroupIdbyObjectId(Long objectId);

    List<CrObjectGroupLines> getAllByGroupId(Long groupId);


//    @Transactional
//    @Query("DELETE FROM CrObjectGroupLines c WHERE c.objectGroupLineId = :objectGroupLineId")
//    public void deleteByObjectGroupLineId(Long objectGroupLineId);

    @Transactional
	public String deleteLinesByobjGrpLineId(Long objGrpLineId);
    @Query("select cpo from CrProjectsObjects cpo\n" +
            "where parentObjectCode = :parentObjectCode and projectId = :projectId and not exists (select objectId \n" +
            "from CrObjectGroupLines gl\n" +
            "where gl.objectId = cpo.objectId)")
    List<CrProjectsObjects> getUngroupedObjects(Long projectId, String parentObjectCode);


}
