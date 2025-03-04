package com.rite.products.convertrite.multitenancy.config.tenant;

import com.rite.products.convertrite.multitenancy.config.tenant.hibernate.DynamicDataSourceBasedMultiTenantSpring;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@Lazy(false)
@Configuration
@EnableJpaRepositories
public class TenantDataSourceConfig {

    @Bean
    public DynamicDataSourceBasedMultiTenantSpring tenantDataSource() {
        return new DynamicDataSourceBasedMultiTenantSpring();
    }

}
