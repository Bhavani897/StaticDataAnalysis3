package com.rite.products.convertrite.respository;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import javax.transaction.Transactional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.model.XxrLookupSets;
import com.rite.products.convertrite.po.LookUpSearchReqPo;

@Repository
public class CloudLookupSearchDaoImpl {

	@PersistenceContext
	private EntityManager entityManager;

	private static final Logger log = LoggerFactory.getLogger(CloudLookupSearchDaoImpl.class);

	@SuppressWarnings("unchecked")
	@Transactional
	public List<XxrLookupSets> searchByOperator(LookUpSearchReqPo lookUpSearchReqPo, HttpHeaders header)
			throws Exception {
		log.info("Start of searchByOperator in Dao######");
		List<XxrLookupSets> li = new ArrayList<>();
		try {

			StringBuilder countSqlBuilder = new StringBuilder("SELECT count(LOOKUP_SET_ID) FROM XXR_LOOKUP_SETS");

			if (lookUpSearchReqPo.getOperator().equalsIgnoreCase("equals")
					&& !Validations.isNullOrEmpty(lookUpSearchReqPo.getLookupSetName()))
				countSqlBuilder.append("  WHERE lower(LOOKUP_SET_NAME)='" + lookUpSearchReqPo.getLookupSetName().toLowerCase() + "'");
			else if (lookUpSearchReqPo.getOperator().equalsIgnoreCase("contains")
					&& !Validations.isNullOrEmpty(lookUpSearchReqPo.getLookupSetName()))
				countSqlBuilder.append("  WHERE  lower(LOOKUP_SET_NAME) LIKE '%" + lookUpSearchReqPo.getLookupSetName().toLowerCase() + "%'");
			Query countQuery = entityManager.createNativeQuery(countSqlBuilder.toString());

			Object count = countQuery.getSingleResult();
			log.info(count + "count:::::::::");
			header.set("count", String.valueOf(count));

			int pageNo = lookUpSearchReqPo.getPageNo() - 1;
			StringBuilder sqlBuilder = new StringBuilder("SELECT * FROM (");
			sqlBuilder.append(
					"SELECT LOOKUP_SET_ID, LOOKUP_SET_NAME, DESCRIPTION, RELATED_TO ,LOOKUP_FLAG, rownum r__  FROM  XXR_LOOKUP_SETS WHERE");
			if (lookUpSearchReqPo.getOperator().equalsIgnoreCase("equals")
					&& !Validations.isNullOrEmpty(lookUpSearchReqPo.getLookupSetName()))
				sqlBuilder.append(" lower(LOOKUP_SET_NAME)='" + lookUpSearchReqPo.getLookupSetName().toLowerCase() + "' and ");
			else if (lookUpSearchReqPo.getOperator().equalsIgnoreCase("contains")
					&& !Validations.isNullOrEmpty(lookUpSearchReqPo.getLookupSetName()))
				sqlBuilder.append("  lower(LOOKUP_SET_NAME) LIKE '%" + lookUpSearchReqPo.getLookupSetName().toLowerCase() + "%' and");
			sqlBuilder.append(
					"  rownum < ((" + lookUpSearchReqPo.getPageNo() + "*" + lookUpSearchReqPo.getPageSize() + ")+1)");
			sqlBuilder.append(" ) WHERE r__ >= (((" + pageNo + ")*" + lookUpSearchReqPo.getPageSize() + ")+1)");

			Query query = entityManager.createNativeQuery(sqlBuilder.toString());

			List<Object> l = query.getResultList();
			l.stream().forEach(x -> {
				Object[] obj = (Object[]) x;
				BigDecimal id = (BigDecimal) obj[0];

				XxrLookupSets xxrLookupSets = new XxrLookupSets();
				xxrLookupSets.setLookUpSetId(id.longValue());
				xxrLookupSets.setLookUpSetName((String) obj[1]);
				xxrLookupSets.setDescription((String) obj[2]);
				xxrLookupSets.setRelatedTo((String) obj[3]);
				xxrLookupSets.setLookupFlag((String) obj[4]);
				li.add(xxrLookupSets);
			});

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return li;
	}
}
