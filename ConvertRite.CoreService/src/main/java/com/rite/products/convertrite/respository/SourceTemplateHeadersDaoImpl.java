package com.rite.products.convertrite.respository;

import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Repository;

import com.opencsv.CSVWriter;
import com.rite.products.convertrite.model.SourceTemplateHeaders;
import com.rite.products.convertrite.po.GetRecordsPostJobExecution;
import com.rite.products.convertrite.po.SourceTemplatePo;
import com.rite.products.convertrite.utils.DataSourceUtil;

@Repository
public class SourceTemplateHeadersDaoImpl {

	@PersistenceContext
	private EntityManager entityManager;

	@Value("${datasource.hostname}")
	private String datasourceHostName;
	@Value("${datasource.port}")
	private int datasourcePort;
	@Value("${spring.datasource.username}")
	private String datasourceUserName;
	@Value("${spring.datasource.password}")
	private String datasourcePassword;
	/*
	 * @Value("${datasource.name}") private String dataSourceName;
	 */

	private static final Logger log = LoggerFactory.getLogger(SourceTemplateHeadersDaoImpl.class);

	@Autowired
	DataSourceUtil dataSourceUtil;

	@SuppressWarnings("unchecked")
	public List<SourceTemplateHeaders> getSourceTemplatesByPo(SourceTemplatePo sourceTemplatePo) throws Exception {

		List<SourceTemplateHeaders> list = new ArrayList<>();
		try {
			StringBuilder sqlBuilder = new StringBuilder().append("select x from SourceTemplateHeaders x");

			if (!isNullOrEmpty(sourceTemplatePo.getObjectCode()))
				sqlBuilder.append(" where x.objectCode = :objectCode");
			if (sourceTemplatePo.getTemplateId() != null)
				sqlBuilder.append(" and x.templateId = :templateId");
			if (sourceTemplatePo.getPodId() != null)
				sqlBuilder.append(" and x.podId = :podId");
			if (sourceTemplatePo.getBu() != null)
				sqlBuilder.append(" and x.bu = :bu");
			if (!isNullOrEmpty(sourceTemplatePo.getBuSpecific()))
				sqlBuilder.append(" and x.buSpecific = :buSpecific");
			if (!isNullOrEmpty(sourceTemplatePo.getParentObjectCode()))
				sqlBuilder.append(" and x.parentObjectCode = :parentObjectCode");
			if (!isNullOrEmpty(sourceTemplatePo.getProjectName()))
				sqlBuilder.append(" and x.projectName = :projectName");
			if (!isNullOrEmpty(sourceTemplatePo.getStagingTableName()))
				sqlBuilder.append(" and x.tableName LIKE :tableName");
			if (!isNullOrEmpty(sourceTemplatePo.getTemplateName()))
				sqlBuilder.append(" and x.templateName LIKE :templateName");
			if (!isNullOrEmpty(sourceTemplatePo.getTemplateType()))
				sqlBuilder.append(" and x.templateType = :templateType");
			if (!isNullOrEmpty(sourceTemplatePo.getViewName()))
				sqlBuilder.append(" and x.viewName LIKE :viewName");
			if (!isNullOrEmpty(sourceTemplatePo.getSourceTableName()))
				sqlBuilder.append(" and x.sourceTableName = :sourceTableName");

			String sql = sqlBuilder.toString();

			/* EntityManager en = em.createEntityManager(); */
			Query query = entityManager.createQuery(sql);

			if (!isNullOrEmpty(sourceTemplatePo.getObjectCode()))
				query.setParameter("objectCode", sourceTemplatePo.getObjectCode());
			if (sourceTemplatePo.getTemplateId() != null)
				query.setParameter("templateId", sourceTemplatePo.getTemplateId());
			if (sourceTemplatePo.getPodId() != null)
				query.setParameter("podId", sourceTemplatePo.getPodId());
			if (sourceTemplatePo.getBu() != null)
				query.setParameter("bu", sourceTemplatePo.getBu());
			if (!isNullOrEmpty(sourceTemplatePo.getBuSpecific()))
				query.setParameter("buSpecific", sourceTemplatePo.getBuSpecific());
			if (!isNullOrEmpty(sourceTemplatePo.getParentObjectCode()))
				query.setParameter("parentObjectCode", sourceTemplatePo.getParentObjectCode());
			if (!isNullOrEmpty(sourceTemplatePo.getProjectName()))
				query.setParameter("projectName", sourceTemplatePo.getProjectName());
			if (!isNullOrEmpty(sourceTemplatePo.getStagingTableName()))
				query.setParameter("tableName", "%" + sourceTemplatePo.getStagingTableName() + "%");
			if (!isNullOrEmpty(sourceTemplatePo.getTemplateName()))
				query.setParameter("templateName", "%" + sourceTemplatePo.getTemplateName() + "%");
			if (!isNullOrEmpty(sourceTemplatePo.getTemplateType()))
				query.setParameter("templateType", sourceTemplatePo.getTemplateType());
			if (!isNullOrEmpty(sourceTemplatePo.getViewName()))
				query.setParameter("viewName", "%" + sourceTemplatePo.getViewName() + "%");
			if (!isNullOrEmpty(sourceTemplatePo.getSourceTableName()))
				query.setParameter("sourceTableName", sourceTemplatePo.getSourceTableName());

			list = query.getResultList();
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		entityManager.clear();
		entityManager.close();
		return list;
	}

	private boolean isNullOrEmpty(String string) {
		return string == null || string.isEmpty();
	}

	@SuppressWarnings("unchecked")
	public List<Object> getSourceFields(String columnName, String viewName) throws Exception {
		log.info("Start of getSourceFields Method in DaoImpl######");
		List<Object> sourceFields = new ArrayList<>();
		try {
			StringBuilder sqlBuilder = new StringBuilder().append("select");
			sqlBuilder.append("  distinct(" + columnName + ")");
			sqlBuilder.append(" from " + viewName);
			String sql = sqlBuilder.toString();
			log.info("sql#######" + sql);
			Query query = entityManager.createNativeQuery(sql);
			sourceFields = query.getResultList();
			/*
			 * if (sourceFields != null) sourceFields.removeIf(Objects::isNull);
			 */
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		entityManager.clear();
		entityManager.close();

		return sourceFields;
	}

	public void getRecordsPostJobExecution(GetRecordsPostJobExecution getRecordsPostJobExecution, String tableName,
			HttpServletResponse response) throws Exception {
		log.info("Start of getRecordsPostJobExecution Method in DaoImpl######");
		ResultSet rs = null;
		Connection con = null;
		PrintWriter writer = response.getWriter();
		String status = getRecordsPostJobExecution.getStatus();
		String batchName = getRecordsPostJobExecution.getBatchName();
		String type = getRecordsPostJobExecution.getType();
		try {
			// count of Records
			if (getRecordsPostJobExecution.getType().equals("JSON")) {
				StringBuilder countSqlBuilder = new StringBuilder(
						"SELECT count(*) FROM " + tableName + " where XXR_BATCH_NAME='" + batchName + "'");
				if (status != null && !status.isBlank() && !status.equalsIgnoreCase("all")) {
					countSqlBuilder.append("  and VALIDATION_FLAG=" + "\'" + status + "\'");
				}
				Query countQuery = entityManager.createNativeQuery(countSqlBuilder.toString());
				Object count = countQuery.getSingleResult();
				log.info(count + "count:::::::::");
				response.setHeader("count", String.valueOf(count));
			}
			StringBuilder sqlBuilder = new StringBuilder();
			if (getRecordsPostJobExecution.getType().equals("JSON"))
				sqlBuilder.append("select * from (");
			sqlBuilder.append(" select a.* ");
			if (getRecordsPostJobExecution.getType().equals("JSON"))
				sqlBuilder.append(", rownum r_");
			sqlBuilder.append(" from " + tableName + " a where a.XXR_BATCH_NAME='" + batchName + "'");
			if (status != null && !status.isBlank() && !status.equalsIgnoreCase("all")) {
				sqlBuilder.append("  and a.VALIDATION_FLAG=" + "\'" + status + "\'");
			}
			if (getRecordsPostJobExecution.getType().equals("JSON")) {
				sqlBuilder.append("  and rownum < ((" + getRecordsPostJobExecution.getPageNo() + "*"
						+ getRecordsPostJobExecution.getPageSize() + ")+1)");
				sqlBuilder.append(" ) WHERE r_ >= (((" + getRecordsPostJobExecution.getPageNo() + "- 1)*"
						+ getRecordsPostJobExecution.getPageSize() + ")+1)");
			}
			String sql = sqlBuilder.toString();
			log.info(sql + ":::::Sql");
			// step2 create connection
			con = dataSourceUtil.createConnection();

			// step3 create the statement object
			PreparedStatement stmt = con.prepareStatement(sql);

			// step4 execute query
			rs = stmt.executeQuery();
			if (type.equalsIgnoreCase("JSON")) {
				response.setContentType("application/json");
				response.setCharacterEncoding("UTF-8");

				JSONArray json = new JSONArray();
				ResultSetMetaData rsmd = rs.getMetaData();

				while (rs.next()) {
					int numColumns = rsmd.getColumnCount();
					JSONObject obj = new JSONObject() {
						/**
						 * changes the value of JSONObject.map to a LinkedHashMap in order to maintain
						 * order of keys.
						 */
						@Override
						public JSONObject put(String key, Object value) throws JSONException {
							try {
								Field map = JSONObject.class.getDeclaredField("map");
								map.setAccessible(true);
								Object mapValue = map.get(this);
								if (!(mapValue instanceof LinkedHashMap)) {
									map.set(this, new LinkedHashMap<>());
								}
							} catch (NoSuchFieldException | IllegalAccessException e) {
								throw new RuntimeException(e);
							}
							return super.put(key, value);
						}
					};

					for (int i = 1; i < numColumns + 1; i++) {
						String column_name = rsmd.getColumnName(i);
						// System.out.println(column_name+":::::::columnName");
						/*
						 * if (rsmd.getColumnType(i) == java.sql.Types.ARRAY) { obj.put(column_name,
						 * rs.getArray(column_name)!=null?rs.getArray(column_name):JSONObject.NULL); }
						 * else if (rsmd.getColumnType(i) == java.sql.Types.BIGINT) {
						 * obj.put(column_name, rs.getInt(column_name)); } else if
						 * (rsmd.getColumnType(i) == java.sql.Types.BOOLEAN) { obj.put(column_name,
						 * rs.getBoolean(column_name)); } else if (rsmd.getColumnType(i) ==
						 * java.sql.Types.BLOB) { obj.put(column_name,
						 * rs.getBlob(column_name)!=null?rs.getBlob(column_name):JSONObject.NULL); }
						 * else if (rsmd.getColumnType(i) == java.sql.Types.DOUBLE) {
						 * obj.put(column_name, rs.getDouble(column_name)); } else if
						 * (rsmd.getColumnType(i) == java.sql.Types.FLOAT) { obj.put(column_name,
						 * rs.getFloat(column_name)); } else if (rsmd.getColumnType(i) ==
						 * java.sql.Types.INTEGER) { obj.put(column_name, rs.getInt(column_name)); }
						 * else if (rsmd.getColumnType(i) == java.sql.Types.NVARCHAR) {
						 * obj.put(column_name,
						 * rs.getNString(column_name)!=null?rs.getNString(column_name):JSONObject.NULL);
						 * } else if (rsmd.getColumnType(i) == java.sql.Types.VARCHAR) {
						 * obj.put(column_name,rs.getString(column_name)!=null?rs.getString(column_name)
						 * :JSONObject.NULL); } else if (rsmd.getColumnType(i) ==
						 * java.sql.Types.TINYINT) { obj.put(column_name, rs.getInt(column_name)); }
						 * else if (rsmd.getColumnType(i) == java.sql.Types.SMALLINT) {
						 * obj.put(column_name, rs.getInt(column_name)); } else if
						 * (rsmd.getColumnType(i) == java.sql.Types.DATE) { obj.put(column_name,
						 * rs.getDate(column_name)!=null?rs.getDate(column_name):JSONObject.NULL); }
						 * else if (rsmd.getColumnType(i) == java.sql.Types.TIMESTAMP) {
						 * obj.put(column_name,
						 * rs.getTimestamp(column_name)!=null?rs.getTimestamp(column_name):JSONObject.
						 * NULL); } else {
						 */
						obj.put(column_name,
								rs.getObject(column_name) != null ? rs.getObject(column_name) : JSONObject.NULL);
						/* } */
					}
					json.put(obj);
					// System.out.println("JsonObject:::::"+obj);
				}
				// System.out.println("JsonArray:::::"+json);
				writer.print(json);
				writer.flush();

			} else if (type.equalsIgnoreCase("CSV")) {
				response.setContentType("text/csv");
				if (tableName != null)
					response.setHeader(HttpHeaders.CONTENT_DISPOSITION,
							"attachment; filename=\"" + tableName + ".csv" + "\"");
				CSVWriter csvWriter = new CSVWriter(writer, '|', CSVWriter.NO_QUOTE_CHARACTER,
						CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);

				csvWriter.writeAll(rs, true);
				csvWriter.flush();
				csvWriter.close();
			}

		} catch (Exception e) {
			// e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			if (writer != null)
				writer.close();
			// step5 close the connection object
			if (con != null)
				con.close();
		}

	}

}
