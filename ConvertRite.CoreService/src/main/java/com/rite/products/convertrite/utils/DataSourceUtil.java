package com.rite.products.convertrite.utils;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.rite.products.convertrite.po.EbsConnectionReqPo;
import java.sql.Connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import oracle.jdbc.pool.OracleDataSource;

@Component
public class DataSourceUtil {

	private static final Logger log = LoggerFactory.getLogger(DataSourceUtil.class);

	@Value("${datasource.hostname}")
	private String datasourceHostName;
	@Value("${datasource.port}")
	private int datasourcePort;
	@Value("${spring.datasource.username}")
	private String datasourceUserName;
	@Value("${spring.datasource.password}")
	private String datasourcePassword;
	@Value("${datasource.sid}")
	private String dataSourceName;


	public Connection createConnection() throws Exception {
		Connection con = null;
		try {
			// System.out.println("datasourceHostName:::::"+datasourceHostName+"datasourceUserName::::::"+datasourceUserName+"datasourcePassword::::"+datasourcePassword+"dataSourceName:::::"+dataSourceName+"datasourcePort::"+datasourcePort);
			Class.forName("oracle.jdbc.driver.OracleDriver");
			OracleDataSource dataSource = new OracleDataSource();
			dataSource.setServerName(datasourceHostName);
			dataSource.setUser(datasourceUserName);
			dataSource.setPassword(datasourcePassword);
			dataSource.setServiceName(dataSourceName);
			dataSource.setPortNumber(datasourcePort);
			dataSource.setDriverType("thin");
			con = dataSource.getConnection();
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new Exception(e.getMessage());
		}
		return con;
	}
	public   Connection createMsSQLConnection(EbsConnectionReqPo ebsConnectionReqPo) throws Exception{
		Connection con = null;
		try{
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			SQLServerDataSource dataSource = new SQLServerDataSource();
			dataSource.setUser(ebsConnectionReqPo.getTargetUserName());
			dataSource.setPassword(ebsConnectionReqPo.getTargetPassword());
			dataSource.setServerName(ebsConnectionReqPo.getHostName());
			dataSource.setDatabaseName(ebsConnectionReqPo.getDatabaseName());
			dataSource.setPortNumber(ebsConnectionReqPo.getPortNumber());
			con = dataSource.getConnection();
		} catch (Exception e) {
			//e.printStackTrace();
			//	throw new Exception("Error While Connecting To Target Mssql Database: "+e.getMessage());
		}
		return con;
	}
}
