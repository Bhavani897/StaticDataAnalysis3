package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.po.CrUserHookResPo;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import com.rite.products.convertrite.model.CrUserHooks;

public interface CrUserHooksRepo extends JpaRepository<CrUserHooks, Long>{
	
	@Query("Select C From CrUserHooks C Where C.hookCode = :hookCode")
	public CrUserHooks findByUserHookCode(String hookCode);


	@Query("select new com.rite.products.convertrite.po.CrUserHookResPo(chu.usageType,cuh.hookText,cuh.description,cuh.hookName) from CrUserHooks cuh,CrHookUsages chu where cuh.hookId=chu.hookId and cuh.hookType='JAVA_API' and chu.templateId= :templateId")
	public CrUserHookResPo fndByHookType(Long templateId);

	boolean existsByHookCode(String hookCode);

}
