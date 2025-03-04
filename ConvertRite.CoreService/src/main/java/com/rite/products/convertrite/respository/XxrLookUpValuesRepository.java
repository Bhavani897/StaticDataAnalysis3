package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.rite.products.convertrite.model.XxrLookUpValues;
import com.rite.products.convertrite.po.BuLovPo;
import com.rite.products.convertrite.po.GantChatResPo;
import com.rite.products.convertrite.po.LovValuesPo;
import com.rite.products.convertrite.po.ObjectCodeLovPo;
import com.rite.products.convertrite.po.TaskBreakDownPo;

public interface XxrLookUpValuesRepository extends JpaRepository<XxrLookUpValues, Long> {

	@Query("select c from XxrLookUpValues c where c.lookUpSetId=:lookUpSetId")
	public List<XxrLookUpValues> getLookupSetValues(@Param("lookUpSetId") Long lookUpSetId);

	@Query("select distinct(a.podId),b.lookUpValue from XxrProjects a, XxrLookUpValues b,XxrLookupSets c where c.lookUpSetId=b.lookUpSetId and lower(c.lookUpSetName)='pod' and a.podId=b.lookUpValueId and a.projectStatus != null and lower(a.projectStatus) !='closed'")
	public List<Object> getPods();

	@Query("select c.lookUpValue  from XxrLookUpValues c where c.lookUpValueId=:id")
	public String getValueById(@Param("id") long id);


	@Query("select c.lookUpValueId  from XxrLookUpValues c where c.lookUpValue=:value")
	public Long getIdByValue(@Param("value") String value);

	@Query("select c.lookUpValueId  from XxrLookUpValues c where c.lookUpValue=:value and c.lookUpSetId=:lookUpSetId")
	public Long getIdByValuesetId(@Param("value") String value, @Param("lookUpSetId") Long lookUpSetId);

	@Query("select  new com.rite.products.convertrite.po.LovValuesPo(b.lookUpValue,a.projectId) from XxrProjects a, XxrLookUpValues b,XxrLookupSets c where c.lookUpSetId=b.lookUpSetId and lower(c.lookUpSetName)='project' and a.projectId=b.lookUpValueId and a.podId=:podId")
	public List<LovValuesPo> getProjectsLov(@Param("podId") Long podId);

	/*
	 * @Query("select  new com.rite.products.convertrite.po.LovValuesPo(b.lookUpValue,a.projectId) from XxrProjects a, XxrLookUpValues b,XxrLookupSets c where c.lookUpSetId=b.lookUpSetId and lower(c.lookUpSetName)='project' and a.projectId=b.lookUpValueId and a.podId=:podId and b.lookUpValue=:projectName"
	 * ) public LovValuesPo projectByPod(@Param("podId") Long
	 * podId,@Param("projectName") String projectName);
	 */
	@SuppressWarnings("rawtypes")
	@Query(value = "select distinct(a.Parent_Object_Id),b.lookup_Value from xxr_proj_activities a, XXR_LookUP_Values b,xxr_lookup_sets c where b.lookup_set_id=c.lookup_set_id and lower(c.lookup_set_name)='parent object code' and a.Parent_Object_Id= b.LookUp_Value_Id   and a.project_Id=:projectId", nativeQuery = true)
	public List getParentObjectLov(@Param("projectId") Long projectId);

	@Query("select new com.rite.products.convertrite.po.ObjectCodeLovPo(b.lookUpValueId,b.lookUpValue) from XxrLookupSets a, XxrLookUpValues b where a.lookUpSetId=b.lookUpSetId and b.actualValue=:parentObjectCode and lower(a.lookUpSetName)=lower('OBJECT CODE')")
	public List<ObjectCodeLovPo> getObjectLov(@Param("parentObjectCode") String parentObjectCode);
	
	@Query("select new com.rite.products.convertrite.po.ObjectCodeLovPo(b.lookUpValueId,b.lookUpValue) from XxrLookupSets a, XxrLookUpValues b where a.lookUpSetId=b.lookUpSetId  and lower(a.lookUpSetName)=lower('OBJECT CODE')")
	public List<ObjectCodeLovPo> getAllObjectLov();

	@Query("select new com.rite.products.convertrite.po.BuLovPo(b.lookUpValueId,b.lookUpValue) from XxrLookupSets a, XxrLookUpValues b where a.lookUpSetId=b.lookUpSetId  and lower(a.lookUpSetName)=lower('BU')")
	public List<BuLovPo> getBuLov();

	@Query("select new com.rite.products.convertrite.po.TaskBreakDownPo(a.taskId,a.startDate,a.endDate,b.lookUpValue,a.completePercentage,a.startDate,a.endDate,a.taskName,a.taskId, d.taskId, d.taskNum)   from XxrActivities a  JOIN XxrActivities d on a.preReqTask=d.taskNum and d.projectId=a.projectId and a.podId=d.podId LEFT JOIN XxrLookUpValues b  on a.taskOwnerId=b.lookUpValueId INNER JOIN XxrProjects c on a.projectId=c.projectId  and  a.podId=c.podId where  c.projectId =:projectId and c.podId =:podId and a.preReqTask is not null order by d.taskId,a.startDate asc ")
	public List<TaskBreakDownPo> getTaskBreakDown(@Param("projectId") Long projectId, @Param("podId") Long podId);
	
	@Query("select new com.rite.products.convertrite.po.GantChatResPo(p.taskId , p.startDate , p.endDate , u.name,p.completePercentage,p.startDate,p.endDate,p.taskName,p.taskId,p1.taskId,p1.taskNum,d.startDate,d.endDate) from XxrActivities p  inner JOIN XxrActivities p1 on ( p.preReqTask = p1.taskNum AND p1.projectId = p.projectId AND p.podId = p1.podId  and p.parentObjectId = p1.parentObjectId)left outer join XxrUsers u on ( p.taskOwnerId = u.id )join XxrProjects d on (d.projectId = p.projectId and d.podId=p.podId) where u.id = :userId  and (p.preReqTask  is not null)order by p1.taskId,p.startDate asc")
	public List<GantChatResPo> getGanttChat(@Param("userId") long userId);

	@Query("select c from XxrLookUpValues c where c.lookUpSetId=:lookUpSetId and lower(c.lookUpValue) LIKE CONCAT('%',:lookUpValue,'%')")
	public List<XxrLookUpValues> getlookupvaluesByVal(Long lookUpSetId, String lookUpValue);

	@Query("select max(cast(c.attribute1 as integer)) from XxrLookUpValues c where c.lookUpSetId =:id")
	public int getMaxAttributeValue(@Param("id") long id);

	@Query("select max(cast(c.attribute1 as integer ))from XxrLookUpValues c where  c.lookUpSetId =:id and c.actualValue =:actualValue")
	public Long getMaxObjectValue(@Param("id") long id,@Param("actualValue") String actualValue);

	@Query("select new com.rite.products.convertrite.po.LovValuesPo(v.lookUpValue , v.lookUpValueId, s.lookUpSetName  ) from XxrLookUpValues v,XxrLookupSets s where  s.lookUpSetId = v.lookUpSetId and  s.lookUpSetName in('Task Name',:columnId)")
	public List<LovValuesPo> getLookUpValues(@Param("columnId") String columnId);

}
