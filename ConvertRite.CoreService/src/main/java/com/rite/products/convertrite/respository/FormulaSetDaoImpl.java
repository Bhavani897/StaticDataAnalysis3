package com.rite.products.convertrite.respository;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.rite.products.convertrite.po.TestDataResPo;
import com.rite.products.convertrite.utils.DataSourceUtil;



@Repository
public class FormulaSetDaoImpl {

	@PersistenceContext
	private EntityManager entityManager;
	
	@Autowired
	DataSourceUtil dataSourceUtil;

	/*
	 * @Value("${datasource.hostname}") private String datasourceHostName;
	 * 
	 * @Value("${datasource.port}") private int datasourcePort;
	 * 
	 * @Value("${spring.datasource.username}") private String datasourceUserName;
	 * 
	 * @Value("${spring.datasource.password}") private String datasourcePassword;
	 * 
	 * @Value("${datasource.name}") private String dataSourceName;
	 */

	private static final Logger log = LoggerFactory.getLogger(FormulaSetDaoImpl.class);

	/*
	 * @SuppressWarnings("unused") public String testSqlSyntax(String sqlQuery)
	 * throws Exception { log.info("Start of testSqlSyntax in Dao Layer #####");
	 * Connection con = null; ResultSet rs = null; String msg = ""; try {
	 * Class.forName("oracle.jdbc.driver.OracleDriver"); OracleDataSource dataSource
	 * = new OracleDataSource(); dataSource.setServerName(datasourceHostName);
	 * dataSource.setUser(datasourceUserName);
	 * dataSource.setPassword(datasourcePassword);
	 * dataSource.setDatabaseName(dataSourceName);
	 * dataSource.setPortNumber(datasourcePort); dataSource.setDriverType("thin");
	 * con = dataSource.getConnection(); PreparedStatement stmt =
	 * con.prepareStatement(sqlQuery); // step4 execute query rs =
	 * stmt.executeQuery(); msg = "Valid sql Syntax";
	 * 
	 * } catch (SQLSyntaxErrorException e) { msg = "Invalid sql Syntax";
	 * e.printStackTrace(); System.out.println(e + "Syntax error"); } catch
	 * (Exception e) { throw new Exception(); }finally { if (con != null)
	 * con.close(); } return msg; }
	 */
	
	public TestDataResPo testSqlData(String sqlQuery) throws Exception {
		log.info("Start of testSqlData in Dao Layer #####");
		Connection con = null;
		ResultSet rs = null;
		TestDataResPo testDataResPo = new TestDataResPo();
		try {
			con = dataSourceUtil.createConnection();
			Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			// step4 execute query
			rs = stmt.executeQuery(sqlQuery);
			int size = 0;
			if (rs.last()) {
				// moves cursor to the last row
				size = rs.getRow(); // get row id
				rs.beforeFirst();
			}
			List<Map<String,Object>> list =new ArrayList<>();
			int columnsCount =0;
			while (rs.next()) {
				columnsCount= rs.getMetaData().getColumnCount();
				Map<String,Object> map=new HashMap<>();
				for (int i = 0; i < columnsCount; i++) {
					map.put(rs.getMetaData().getColumnLabel(i + 1).toLowerCase(), rs.getObject(i + 1));
				}
				list.add(map);
			}
			log.info("columnsCount:::::"+columnsCount);
			if (size == 1 && columnsCount ==1) {
				testDataResPo.setMessage("Valid Data");
				testDataResPo.setValidData(true);
			} else {
				testDataResPo.setMessage("Invalid Data");
				testDataResPo.setValidData(false);
			}
			testDataResPo.setData(list);
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}finally {
			if (con != null)
				con.close();
		}
		return testDataResPo;
	}

}
