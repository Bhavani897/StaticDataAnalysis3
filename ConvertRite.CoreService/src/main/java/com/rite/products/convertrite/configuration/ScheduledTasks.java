package com.rite.products.convertrite.configuration;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import com.rite.products.convertrite.model.CrCloudJobStatus;
import com.rite.products.convertrite.multitenancy.model.Pod;
import com.rite.products.convertrite.multitenancy.repository.PodRepository;
import com.rite.products.convertrite.po.CustomRestApiReqPo;
import com.rite.products.convertrite.respository.CrCloudJobStatusRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.rite.products.convertrite.model.XxrCloudDataProcess;
import com.rite.products.convertrite.model.XxrCloudDataProcessConfig;
import com.rite.products.convertrite.respository.XxrCloudDataProcessConfigRepository;
import com.rite.products.convertrite.respository.XxrCloudDataProcessRepository;
import org.springframework.web.multipart.support.AbstractMultipartHttpServletRequest;

import javax.servlet.http.HttpServletRequest;

import static com.rite.products.convertrite.multitenancy.interceptor.TenantInterceptor.X_TENANT_ID;

@Component
public class ScheduledTasks {

	private static final Logger log = LoggerFactory.getLogger(ScheduledTasks.class);

	private static final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");


	@Value("${clouddataprocess-url}")
	private String url;
	@Autowired
	private PodRepository podRepository;
	@Value("${scheduled-job-url}")
	private String scheduledJobUrl;
	@Autowired
	RestTemplate restTemplate;

	@Scheduled(fixedRateString = "${sync-schedule-job-milliseconds}")
	public void scheduleSyncJob(){
		List<Pod> podsLi=podRepository.findByScheduledJobFlag("Y");
		for(Pod pod: podsLi) {
			HttpHeaders headers = new HttpHeaders();
			headers.set("Accept", MediaType.APPLICATION_JSON_VALUE);
			headers.set("X-TENANT-ID", String.valueOf(pod.getPodId()));
			//Calling RestApi to get Ess Job status & sync back tables
			HttpEntity<String> requestEntity = new HttpEntity<>(headers);
			//RestTemplate restTemplate = new RestTemplate();
			ResponseEntity<?> response = restTemplate.exchange(scheduledJobUrl, HttpMethod.POST, requestEntity, Void.class);
		}
	}
}