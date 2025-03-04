package com.rite.products.convertrite.multitenancy.config.tenant.hibernate;

import com.rite.products.convertrite.multitenancy.model.Pod;
import com.rite.products.convertrite.multitenancy.repository.PodRepository;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ResourceLoaderAware;
import org.springframework.core.io.ResourceLoader;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;

/**
 * Based on MultiTenantSpringLiquibase, this class provides Liquibase support for
 * multi-tenancy based on a dynamic collection of DataSources.
 */
@Getter
@Setter
@Slf4j
public class DynamicDataSourceBasedMultiTenantSpring implements InitializingBean, ResourceLoaderAware {

    @Autowired
    private PodRepository tenantRepository;

    private ResourceLoader resourceLoader;

    @Override
    public void afterPropertiesSet() throws Exception {
        log.info("DynamicDataSources based multitenancy enabled");
        Long podId= 33L;
        this.runOnAllTenants(tenantRepository.findAllById(Collections.singleton(podId)));
       //   this.runOnAllTenants(tenantRepository.findAll());
    }

    protected void runOnAllTenants(Collection<Pod> tenants) {
        for(Pod tenant : tenants) {
            log.info("Initializing dynamic datasource for tenant " + tenant.getPodId());
            try (Connection connection = DriverManager.getConnection(tenant.getPodTargetUrl(), tenant.getPodDbUser(), tenant.getPodDbPassword())) {
                DataSource tenantDataSource = new SingleConnectionDataSource(connection, false);
            } catch (Exception e) {
                log.error("Failed to initialize dynamic datasource for tenant " + tenant.getPodId(), e);
            }
            log.info("Dynamic datasource ran for tenant " + tenant.getPodId());
        }
    }

}
