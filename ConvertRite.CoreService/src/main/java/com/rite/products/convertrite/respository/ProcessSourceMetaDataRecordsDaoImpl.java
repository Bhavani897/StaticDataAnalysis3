package com.rite.products.convertrite.respository;

import javax.persistence.EntityManager;
import javax.persistence.ParameterMode;
import javax.persistence.PersistenceContext;
import javax.persistence.StoredProcedureQuery;
import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

@Repository

public class ProcessSourceMetaDataRecordsDaoImpl {

	private static final Logger log = LoggerFactory.getLogger(ProcessSourceMetaDataRecordsDaoImpl.class);

	@PersistenceContext
	private EntityManager entityManager;

	@Transactional
	public String processSourceRecords(String fileName, String metaDataFileType, HttpServletRequest request)
			throws Exception {
		log.info("Start of processSourceRecords Method in DaoImpl### ");
		String status = "";
		try {
			log.info("UserId#######"+request.getHeader("userId"));
			if ("TABLE".equalsIgnoreCase(metaDataFileType)) {
				StoredProcedureQuery createTableRec = entityManager
						.createStoredProcedureQuery("XXR_SOURCE_PKG.XXR_METADATA_TABLE_PRC")
						.registerStoredProcedureParameter("p_file", String.class, ParameterMode.IN)
						.registerStoredProcedureParameter("p_user_id", String.class, ParameterMode.IN)
						.registerStoredProcedureParameter("p_status", String.class, ParameterMode.OUT)
						.setParameter("p_file", fileName).setParameter("p_user_id", request.getHeader("userId"));
				createTableRec.execute();
				String tableStatus = (String) createTableRec.getOutputParameterValue("p_status");
				log.info("tableStatus" + tableStatus);
				/*
				 * status = tableStatus.equalsIgnoreCase("Y") ?
				 * "Processed sourceTable metadata records succesfully" :
				 * "Something went wrong while processing records";
				 * 
				 */
				status = tableStatus;
			} else if ("COLUMN".equalsIgnoreCase(metaDataFileType)) {
				StoredProcedureQuery createColumnRec = entityManager
						.createStoredProcedureQuery("XXR_SOURCE_PKG.XXR_METADATA_COLUMN_PRC")
						.registerStoredProcedureParameter("p_file", String.class, ParameterMode.IN)
						.registerStoredProcedureParameter("p_user_id", String.class, ParameterMode.IN)
						.registerStoredProcedureParameter("p_status", String.class, ParameterMode.OUT)
						.setParameter("p_file", fileName).setParameter("p_user_id", request.getHeader("userId"));
				createColumnRec.execute();
				String columnStatus = (String) createColumnRec.getOutputParameterValue("p_status");
				log.info("columnStatus" + columnStatus);
				status = columnStatus;
			}

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		return status;
	}

}
