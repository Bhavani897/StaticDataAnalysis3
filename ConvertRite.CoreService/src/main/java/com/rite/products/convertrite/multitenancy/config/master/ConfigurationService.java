package com.rite.products.convertrite.multitenancy.config.master;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.util.Arrays;

@Configuration
@Slf4j
public class ConfigurationService {
    @Value("${get-data-source-details}")
    String getDataSourceDetailsUrl;
    private final RestTemplate restTemplate;

    public ConfigurationService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public DataSourceProperties fetchDataSourceProperties() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode payload = null;
        HttpHeaders header = new HttpHeaders();
        header.setAccept(Arrays.asList(MediaType.APPLICATION_JSON));
        HttpEntity<String> entity = new HttpEntity<String>(header);
        ResponseEntity<String> objects = restTemplate.exchange(getDataSourceDetailsUrl, HttpMethod.GET, entity, String.class);
        payload = mapper.readTree(objects.getBody()).path("payload");
        log.info("Response-->" + payload);
        ObjectMapper objectMapper = new ObjectMapper();
        DataSourceProperties dataSourcePropertiesPo = objectMapper.readValue(payload.toString(), DataSourceProperties.class);
        return dataSourcePropertiesPo;
    }
}
