package com.rite.products.convertrite.respository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import javax.persistence.EntityManager;
import javax.persistence.ParameterMode;
import javax.persistence.PersistenceContext;
import javax.persistence.StoredProcedureQuery;
import javax.transaction.Transactional;

import org.json.CDL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.po.ReconcileDetailsReqPo;
import com.rite.products.convertrite.po.ReconcileDetailsResPo;
import com.rite.products.convertrite.po.ReconcileErrorReqPo;
import com.rite.products.convertrite.po.ReconcileErrorResPo;
import com.rite.products.convertrite.utils.DataSourceUtil;
import com.rite.products.convertrite.utils.Utils;

@Repository
public class ReconcileDaoImpl {

	private static final Logger log = LoggerFactory.getLogger(ReconcileDaoImpl.class);

	@PersistenceContext
	private EntityManager entityManager;

	@Autowired
	DataSourceUtil dataSourceUtil;

	@Transactional
	public void recocileProc(String cldTemplateName, String batchName) {
		log.info("Start of recocileProc Method in DaoImpl### ");
		StoredProcedureQuery reconcileProc = entityManager
				.createStoredProcedureQuery("xxr_reconcile_utils_pkg.xxr_reconcile_prc")
				.registerStoredProcedureParameter("p_template_name", String.class, ParameterMode.IN)
				.registerStoredProcedureParameter("p_batch_name", String.class, ParameterMode.IN)
				.setParameter("p_template_name", cldTemplateName).setParameter("p_batch_name", batchName);
		reconcileProc.execute();
		entityManager.clear();
		entityManager.close();
	}

	public ReconcileDetailsResPo getReconcileDetails(ReconcileDetailsReqPo reconcileDetailsReqPo) throws Exception {
		log.info("Start of getReconcileDetails in dao #######");
		ReconcileDetailsResPo reconcileDetailsResPo = new ReconcileDetailsResPo();
		ResultSet rs = null;
		Connection con = null;
		try {
            String sql = "SELECT xxr_reconcile_utils_pkg.conversion_report_filegen(" +
                    reconcileDetailsReqPo.getCldTemplateId() + ",'" + reconcileDetailsReqPo.getBatchName()
                    + "'," + reconcileDetailsReqPo.getPageNo() + "," + reconcileDetailsReqPo.getPageSize() + ")" +
                    " from dual";
			con = dataSourceUtil.createConnection();
			// con = dataSource.getConnection();
			// step3 create the statement object
			PreparedStatement stmt = con.prepareStatement(sql);
			// step4 execute query
			rs = stmt.executeQuery();
			// int columnCount=0;
			if (rs.next()) {
				// columnCount= rs.getMetaData().getColumnCount();
				String clobString = Utils.clobToString(rs.getClob(1));
				//clobString=clobString.replaceAll("\\s+","");
				String json =CDL.toJSONArray(clobString).toString();
				reconcileDetailsResPo.setReconcileDetailsClob(json);
				reconcileDetailsResPo.setMessage("Successfully retrieved  reconcile details");
			}
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		} finally {
				con.close();
		}
		return reconcileDetailsResPo;
	}

	public ReconcileErrorResPo getReconcileErrors(ReconcileErrorReqPo reconcileErrorReqPo) throws Exception {
		log.info("Start of getReconcileErrors ######");
		ReconcileErrorResPo reconcileErrorResPo=new ReconcileErrorResPo();
		ResultSet rs = null;
		Connection con = null;
		try {
            String sql = "SELECT xxr_reconcile_utils_pkg.error_rec_clob(" +
                    reconcileErrorReqPo.getCldTemplateId() + ",'" + reconcileErrorReqPo.getBatchName()
                    + "'," + reconcileErrorReqPo.getPageNo() + "," + reconcileErrorReqPo.getPageSize() + ")" +
                    " from dual";
			con = dataSourceUtil.createConnection();
			// con = dataSource.getConnection();
			// step3 create the statement object
			PreparedStatement stmt = con.prepareStatement(sql);
			// step4 execute query
			rs = stmt.executeQuery();
			// int columnCount=0;
			if (rs.next()) {
				// columnCount= rs.getMetaData().getColumnCount();
				String clobString = Utils.clobToString(rs.getClob(1));
				//clobString=clobString.replaceAll("\\s+","");
				String json =CDL.toJSONArray(clobString).toString();
				reconcileErrorResPo.setReconcileErrorClob(json);
				reconcileErrorResPo.setMessage("Successfully retrieved reconcile error details");
			}
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		} finally {
				con.close();
		}
		return reconcileErrorResPo;
	}

}
