package com.rite.products.convertrite.configuration;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.web.firewall.DefaultHttpFirewall;
import org.springframework.security.web.firewall.HttpFirewall;

@Configuration
@EnableWebSecurity
@Slf4j
public class JwtSecurityConfig extends WebSecurityConfigurerAdapter {

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        log.info("entering configure Method####");
        //long startTime = System.currentTimeMillis();
        http.cors().and().csrf(csrf -> csrf.ignoringAntMatchers("/api/convertritecore/**")).authorizeRequests().antMatchers("/fallback").permitAll()
                .antMatchers(HttpMethod.POST, "/callback").permitAll()
                .antMatchers(HttpMethod.POST, "/synccloudinterfacedata").permitAll()
                .antMatchers(HttpMethod.POST, "/clouddataprocessingrequest").permitAll().antMatchers("/actuator/*")
                .permitAll();
        //.anyRequest().authenticated();
}

    @Override
    public void configure(WebSecurity web) {
        // long startTime=System.currentTimeMillis();
        // The new firewall is forced to overwrite the original
        web.httpFirewall(allowUrlEncodedSlashHttpFirewall());
        // long endTime=System.currentTimeMillis();
        // long diff=endTime-startTime;
        // log.debug("TimeDiff ##"+diff);
    }

    @Bean
    public HttpFirewall allowUrlEncodedSlashHttpFirewall() {
        DefaultHttpFirewall firewall = new DefaultHttpFirewall();
        firewall.setAllowUrlEncodedSlash(true);
        return firewall;
    }

}
