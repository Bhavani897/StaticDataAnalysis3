package com.rite.products.convertrite.respository;

import java.math.BigDecimal;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import javax.transaction.Transactional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.model.XxrSrcLookupSets;
import com.rite.products.convertrite.model.XxrSrcLookupValues;
import com.rite.products.convertrite.po.LoadCustomLookupReqPo;
import com.rite.products.convertrite.po.LoadLookupResPo;
import com.rite.products.convertrite.po.LookUpSearchReqPo;

@Repository
public class SourceLookupDaoImpl {
	Logger log = LoggerFactory.getLogger(SourceLookupDaoImpl.class);

	@PersistenceContext
	private EntityManager entityManager;

	@Autowired
	XxrSrcLookupSetRepository xxrSrcLookupSetRepository;
	@Autowired
	XxrSrcLookupValuesRepository xxrSrcLookupValuesRepository;

	@Transactional
	@SuppressWarnings("unchecked")
	public List<XxrSrcLookupSets> searchSrcLookups(LookUpSearchReqPo lookUpSearchReqPo, HttpHeaders header)
			throws Exception {
		log.info("Start of searchSrcLookups in Dao#########");
		List<XxrSrcLookupSets> srcLookupsetsLi = new ArrayList<>();
		try {
			StringBuilder countSqlBuilder = new StringBuilder("SELECT count(LOOKUP_SET_ID) FROM XXR_SRC_LOOKUP_SETS");

			if (lookUpSearchReqPo.getOperator().equalsIgnoreCase("equals")
					&& !Validations.isNullOrEmpty(lookUpSearchReqPo.getLookupSetName()))
				countSqlBuilder.append(
						"  WHERE lower(LOOKUP_SET_NAME)='" + lookUpSearchReqPo.getLookupSetName().toLowerCase() + "'");
			else if (lookUpSearchReqPo.getOperator().equalsIgnoreCase("contains")
					&& !Validations.isNullOrEmpty(lookUpSearchReqPo.getLookupSetName()))
				countSqlBuilder.append("  WHERE  lower(LOOKUP_SET_NAME) LIKE '%"
						+ lookUpSearchReqPo.getLookupSetName().toLowerCase() + "%'");
			Query countQuery = entityManager.createNativeQuery(countSqlBuilder.toString());

			Object count = countQuery.getSingleResult();
			log.info(count + "count:::::::::");
			header.set("count", String.valueOf(count));

			int pageNo = lookUpSearchReqPo.getPageNo() - 1;
			StringBuilder sqlBuilder = new StringBuilder("SELECT * FROM (");
			sqlBuilder.append(
					"SELECT LOOKUP_SET_ID, LOOKUP_SET_NAME, DESCRIPTION, rownum r__  FROM  XXR_SRC_LOOKUP_SETS WHERE");
			if (lookUpSearchReqPo.getOperator().equalsIgnoreCase("equals")
					&& !Validations.isNullOrEmpty(lookUpSearchReqPo.getLookupSetName()))
				sqlBuilder.append(
						" lower(LOOKUP_SET_NAME)='" + lookUpSearchReqPo.getLookupSetName().toLowerCase() + "' and ");
			else if (lookUpSearchReqPo.getOperator().equalsIgnoreCase("contains")
					&& !Validations.isNullOrEmpty(lookUpSearchReqPo.getLookupSetName()))
				sqlBuilder.append("  lower(LOOKUP_SET_NAME) LIKE '%"
						+ lookUpSearchReqPo.getLookupSetName().toLowerCase() + "%' and");
			sqlBuilder.append(
					"  rownum < ((" + lookUpSearchReqPo.getPageNo() + "*" + lookUpSearchReqPo.getPageSize() + ")+1)");
			sqlBuilder.append(" ) WHERE r__ >= (((" + pageNo + ")*" + lookUpSearchReqPo.getPageSize() + ")+1)");

			Query query = entityManager.createNativeQuery(sqlBuilder.toString());

			List<Object> l = query.getResultList();
			l.stream().forEach(x -> {
				Object[] obj = (Object[]) x;
				BigDecimal id = (BigDecimal) obj[0];

				XxrSrcLookupSets xxrLookupSets = new XxrSrcLookupSets();
				xxrLookupSets.setLookupsetId(id.longValue());
				xxrLookupSets.setLookupSetName((String) obj[1]);
				xxrLookupSets.setDescription((String) obj[2]);

				srcLookupsetsLi.add(xxrLookupSets);
			});

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return srcLookupsetsLi;
	}

	@Transactional
	@SuppressWarnings("unchecked")
	public LoadLookupResPo loadLookupsFromSrc(String dbLink) throws Exception {
		log.info("Start of loadLookupsFromSrc###########");
		LoadLookupResPo loadLookupResPo = new LoadLookupResPo();
		Date creationDate = null;
		String loadLookups = "";
		try {
			creationDate = xxrSrcLookupSetRepository.getCreationDate();
			log.info(creationDate + ":::::::creationDate");
			if (creationDate == null)
				loadLookups = "Select DISTINCT LOOKUP_TYPE,LOOKUP_CODE from fnd_lookup_values@" + dbLink;
			else
				loadLookups = "Select DISTINCT LOOKUP_TYPE,LOOKUP_CODE from fnd_lookup_values@" + dbLink
						+ " where  TO_CHAR(LAST_UPDATE_DATE,'YYYY-MM-DD') > '" + creationDate
						+ "' or  TO_CHAR(CREATION_DATE,'YYYY-MM-DD') > '" + creationDate + "'";

			log.info(loadLookups + ":::::::loadLookups");
			Query query = entityManager.createNativeQuery(loadLookups);
			List<Object> li = query.getResultList();

			li.stream().forEach(x -> {
				Object[] obj = (Object[]) x;
				String lookupType = String.valueOf(obj[0]);
				String lookupCode = String.valueOf(obj[1]);
				Long lookupSetId = null;
				// check wether Lookup Type exists or not
				XxrSrcLookupSets srcLookupSets = xxrSrcLookupSetRepository.findByLookupSetName(lookupType);
				if (srcLookupSets != null) {
					lookupSetId = srcLookupSets.getLookupsetId();
				} else {
					XxrSrcLookupSets xxrSrcLookupSets = new XxrSrcLookupSets();
					xxrSrcLookupSets.setLookupSetName(lookupType);
					xxrSrcLookupSets.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
					xxrSrcLookupSets.setCreatedBy("ConvertRite");
					xxrSrcLookupSets.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
					xxrSrcLookupSets.setLastUpdateBy("ConvertRite");
					XxrSrcLookupSets xxrSrcLookupSetRes = xxrSrcLookupSetRepository.save(xxrSrcLookupSets);
					lookupSetId = xxrSrcLookupSetRes.getLookupsetId();
				}
				if (lookupSetId != null) {
					log.info(lookupSetId + "#######lookupSetId");
					XxrSrcLookupValues xxrSrcLookupValues = xxrSrcLookupValuesRepository
							.findByLookupValueAndLookupSetId(lookupCode, lookupSetId);
					Long lookUpId = null;
					if (xxrSrcLookupValues != null) {
						lookUpId = xxrSrcLookupValues.getLookupValueId();
						log.info(lookUpId + "#######lookUpId");
					} else {
						XxrSrcLookupValues srcLookupValues = new XxrSrcLookupValues();
						srcLookupValues.setLookupSetId(lookupSetId);
						srcLookupValues.setLookupValue(lookupCode);
						srcLookupValues.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
						srcLookupValues.setCreatedBy("ConvertRite");
						srcLookupValues.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
						srcLookupValues.setLastUpdateBy("ConvertRite");
						XxrSrcLookupValues xxrSrcLookupValuRes = xxrSrcLookupValuesRepository.save(srcLookupValues);
						log.info(xxrSrcLookupValuRes.getLookupValueId() + "#######lookUpId");
					}
				}
			});

			loadLookupResPo.setMessage("Loaded Lookups Successfully from SourceSystem");
			loadLookupResPo.setCount(li.size());
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
		return loadLookupResPo;
	}

	public LoadLookupResPo loadCustomLookupsFromSrc(LoadCustomLookupReqPo loadCustomLookupReqPo) {
		log.info("Start of loadCustomLookupsFromSrc###########");
		LoadLookupResPo loadLookupResPo = new LoadLookupResPo();
		String sql = loadCustomLookupReqPo.getCustomQuery();
		sql = sql.replaceAll("\\{0\\}", loadCustomLookupReqPo.getDbLink());
		Query query = entityManager.createNativeQuery(sql);
		List<Object> li = query.getResultList();
		li.stream().forEach(x -> {
			Object[] obj = (Object[]) x;
			String lookupType = String.valueOf(obj[0]);
			String lookupCode = String.valueOf(obj[1]);
			Long lookupSetId = null;
			// check wether Lookup Type exists or not
			XxrSrcLookupSets srcLookupSets = xxrSrcLookupSetRepository.findByLookupSetName(lookupType);
			if (srcLookupSets != null) {
				lookupSetId = srcLookupSets.getLookupsetId();
			} else {
				XxrSrcLookupSets xxrSrcLookupSets = new XxrSrcLookupSets();
				xxrSrcLookupSets.setLookupSetName(lookupType);
				xxrSrcLookupSets.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
				xxrSrcLookupSets.setCreatedBy("ConvertRite");
				xxrSrcLookupSets.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
				xxrSrcLookupSets.setLastUpdateBy("ConvertRite");
				XxrSrcLookupSets xxrSrcLookupSetRes = xxrSrcLookupSetRepository.save(xxrSrcLookupSets);
				lookupSetId = xxrSrcLookupSetRes.getLookupsetId();
			}
			if (lookupSetId != null) {
				log.info(lookupSetId + "#######lookupSetId");
				XxrSrcLookupValues xxrSrcLookupValues = xxrSrcLookupValuesRepository
						.findByLookupValueAndLookupSetId(lookupCode, lookupSetId);
				Long lookUpId = null;
				if (xxrSrcLookupValues != null) {
					lookUpId = xxrSrcLookupValues.getLookupValueId();
					log.info(lookUpId + "#######lookUpId");
				} else {
					XxrSrcLookupValues srcLookupValues = new XxrSrcLookupValues();
					srcLookupValues.setLookupSetId(lookupSetId);
					srcLookupValues.setLookupValue(lookupCode);
					srcLookupValues.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
					srcLookupValues.setCreatedBy("ConvertRite");
					srcLookupValues.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
					srcLookupValues.setLastUpdateBy("ConvertRite");
					XxrSrcLookupValues xxrSrcLookupValuRes = xxrSrcLookupValuesRepository.save(srcLookupValues);
					log.info(xxrSrcLookupValuRes.getLookupValueId() + "#######lookUpId");
				}
			}
		});

		loadLookupResPo.setMessage("Loaded Lookups Successfully from SourceSystem");
		loadLookupResPo.setCount(li.size());

		return loadLookupResPo;
	}

}
