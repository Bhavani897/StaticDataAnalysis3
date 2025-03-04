package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrFormulaSets;

@Repository
public interface XxrFormulaSetsRepository extends JpaRepository<XxrFormulaSets, String> {

	List<XxrFormulaSets> findByformulaSetId(Long formulaSetId);

	@Query("select f.formulaSetId from XxrFormulaSets f where lower(f.formulaSetName)=lower(:formulaSetName)")
	public Long getFormulaSetId(@Param("formulaSetName") String formulaSetName);

	List<XxrFormulaSets> findByobjectId(Long objectId);
	
	@Query("select c.formulaSetName from XxrFormulaSets c where c.formulaSetId=:formulaSetId")
	public String getFormulaSetNameNameById(@Param("formulaSetId") Long formulaSetId);
}
