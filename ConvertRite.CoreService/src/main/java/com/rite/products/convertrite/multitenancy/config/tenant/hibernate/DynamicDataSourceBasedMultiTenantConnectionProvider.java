package com.rite.products.convertrite.multitenancy.config.tenant.hibernate;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.rite.products.convertrite.multitenancy.model.Pod;
import com.rite.products.convertrite.multitenancy.repository.PodRepository;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.engine.jdbc.connections.spi.AbstractDataSourceBasedMultiTenantConnectionProviderImpl;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
@Slf4j
@Component
public class DynamicDataSourceBasedMultiTenantConnectionProvider
        extends AbstractDataSourceBasedMultiTenantConnectionProviderImpl {

    private static final long serialVersionUID = -460277105706399638L;

    private static final String TENANT_POOL_NAME_SUFFIX = "DataSource-";

    @Qualifier("masterDataSource")
    private final DataSource masterDataSource;

    @Qualifier("masterDataSourceProperties")
    private final DataSourceProperties dataSourceProperties;

    private final PodRepository masterTenantRepository;

    @Value("${multitenancy.datasource-cache.maximumSize:100}")
    private Long maximumSize;

    @Value("${multitenancy.datasource-cache.expireAfterAccess:10}")
    private Integer expireAfterAccess;

    @Value("${multitenancy.tenant.datasource.hikari.maximumPoolSize}")
    private String maximumPoolSize;

    @Value("${multitenancy.tenant.datasource.hikari.minimumIdle}")
    private String minimumIdle;

    @Value("${multitenancy.tenant.datasource.hikari.idleTimeout}")
    private String idleTimeout;

    private LoadingCache<String, DataSource> tenantDataSources;

    @PostConstruct
    private void createCache() {
        tenantDataSources = Caffeine.newBuilder()
                .maximumSize(maximumSize)
                .expireAfterAccess(expireAfterAccess, TimeUnit.MINUTES)
                .removalListener((RemovalListener<String, DataSource>) (tenantId, dataSource, removalCause) -> {
                    HikariDataSource ds = (HikariDataSource) dataSource;
                    ds.close(); // tear down properly
                    log.info("Closed datasource: {}", ds.getPoolName());
                })
                .build(tenantId -> {
                        Pod tenant = masterTenantRepository.findById(Long.parseLong(tenantId))
                                .orElseThrow(() -> new RuntimeException("No such tenant: " + tenantId));
                        return createAndConfigureDataSource(tenant);
                    }
                );
    }

    @Override
    protected DataSource selectAnyDataSource() {
        return masterDataSource;
    }

    @Override
    protected DataSource selectDataSource(String tenantIdentifier) {
        return tenantDataSources.get(tenantIdentifier);
    }

    private DataSource createAndConfigureDataSource(Pod tenant) {
        try {
            Properties properties = new Properties();
            properties.setProperty("driverClassName", "oracle.jdbc.OracleDriver");
            properties.setProperty("jdbcUrl", tenant.getPodTargetUrl());
            properties.setProperty("username", tenant.getPodDbUser());
            properties.setProperty("password", tenant.getPodDbPassword());
            properties.setProperty("maximumPoolSize", maximumPoolSize);
            properties.setProperty("minimumIdle", minimumIdle);
            properties.setProperty("idleTimeout", idleTimeout);
            properties.setProperty("poolName", TENANT_POOL_NAME_SUFFIX+tenant.getPodId());

            HikariConfig config = new HikariConfig(properties);
            HikariDataSource ds = new HikariDataSource(config);
            return ds;
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return null;
    }

}