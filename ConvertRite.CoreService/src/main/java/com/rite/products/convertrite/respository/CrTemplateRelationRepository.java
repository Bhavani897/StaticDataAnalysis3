package com.rite.products.convertrite.respository;

import com.rite.products.convertrite.model.CrTemplateRelation;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface CrTemplateRelationRepository extends JpaRepository<CrTemplateRelation,Long> {
    List<CrTemplateRelation> findAllByGroupId(Long groupId);

    void deleteAllByGroupId(Long groupId);
}
