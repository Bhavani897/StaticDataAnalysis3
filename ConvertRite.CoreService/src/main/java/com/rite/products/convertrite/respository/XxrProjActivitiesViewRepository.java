package com.rite.products.convertrite.respository;

import java.util.List;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.model.XxrProjActivitiesView;

@Repository
public interface XxrProjActivitiesViewRepository  extends JpaRepository<XxrProjActivitiesView,Long>{
	
	List<XxrProjActivitiesView> findByProjectIdAndPodIdOrderBySeqAsc(Long projectId,Long podId);

	Page<XxrProjActivitiesView> findAllByProjectIdAndPodId(Long projectId, Long podId, Pageable pageable);

}
