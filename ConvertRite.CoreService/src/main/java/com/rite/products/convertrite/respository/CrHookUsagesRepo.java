package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import com.rite.products.convertrite.model.CrHookUsages;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface CrHookUsagesRepo extends JpaRepository<CrHookUsages, Long>{

	@Query("SELECT C From CrHookUsages C Where C.templateId = :templateId")
	public List<CrHookUsages> getHooksByTemplateId(Long templateId);

	@Query("SELECT hdrs.templateName FROM CrCloudTemplateHeaders hdrs, CrHookUsages hks " +
			"WHERE hks.templateId = hdrs.templateId " +
			"AND hks.hookId = :hookId")
	List<String> selectCldNameFromCldTempHds(@Param("hookId") Long hookId);
}
