package com.rite.products.convertrite.multitenancy.config.master;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

@Configuration
public class MasterDataSourceConfig {

    private final ConfigurationService configurationService;
    private DataSourceProperties dataSourceProperties;

    @Autowired
    public MasterDataSourceConfig(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    @PostConstruct
    public void init() throws JsonProcessingException {
        this.dataSourceProperties = configurationService.fetchDataSourceProperties();
    }

    @Bean
    public DataSource masterDataSource() {
        HikariDataSource dataSource = dataSourceProperties
                .initializeDataSourceBuilder()
                .type(HikariDataSource.class)
                .build();
        dataSource.setPoolName("masterDataSource");
        return dataSource;
    }

    @Bean
    public JdbcTemplate jdbcTemplate(DataSource masterDataSource) {
        return new JdbcTemplate(masterDataSource);
    }
}
