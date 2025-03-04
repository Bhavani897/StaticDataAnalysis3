package com.rite.products.convertrite.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rite.products.convertrite.model.CloudLoginDetails;
import com.rite.products.convertrite.model.CrCloudLogDetails;
import com.rite.products.convertrite.respository.CrCloudLogFileRepo;
import com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub;
import com.rite.products.convertrite.stubs.erp.ErpIntegrationServiceStub.*;
import com.rite.products.convertrite.utils.Utils;
import org.apache.axis2.client.ServiceClient;
import org.apache.axis2.transport.http.impl.httpclient4.HttpTransportPropertiesImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import javax.activation.DataHandler;
import java.lang.String;
import java.util.Arrays;


@Service
public class CrErpIntegrationServiceImpl implements CrErpIntegrationService {

    private static final Logger log = LoggerFactory.getLogger(CrErpIntegrationServiceImpl.class);

    @Value("${convertrite-admin-host}")
    String convertriteAdminHost;
    @Value("${erp_url}")
    private String erpurl;

    @Autowired
    Utils utils;

    @Autowired
    CrCloudLogFileRepo crCloudLogFileRepo;

    @Override
    public byte[] downloadESSJobExecutionDetails(String requestId, Long podId, String bearerToken) throws Exception{
        log.info("Start of downloadESSJobExecutionDetails Method#####");

        byte[] buf = null;
        CrCloudLogDetails crCloudLogDetails;
        try {
            crCloudLogDetails = crCloudLogFileRepo.findByLoadRequestId(Long.valueOf(requestId));

            if (crCloudLogDetails != null) {
                buf = crCloudLogDetails.getLogFileZip();
                log.info("zip file length from DB: "+buf.length);
            }
            else {
                buf = getLogFileFromCloud(requestId, podId, bearerToken);
            }
            return buf;
        } catch (Exception e) {
            log.error("Error occurred during downloadESSJobExecutionDetails for requestId :"+requestId+". "+e.getMessage());
            throw new Exception(e.getMessage());
        }
    }

    //Get log file zip from cloud and save it to database
    private byte[] getLogFileFromCloud(String requestId, Long podId, String bearerToken) throws Exception{
        byte[] buf = null;
        try {
            DownloadESSJobExecutionDetails downloadESSJobExecutionDetails = new DownloadESSJobExecutionDetails();
            downloadESSJobExecutionDetails.setRequestId(requestId);
            downloadESSJobExecutionDetails.setFileType("all");

            CloudLoginDetails cloudLoginDetails = getCloudLoginDetails(podId, bearerToken);
            String userName = cloudLoginDetails.getUsername();
            String password = cloudLoginDetails.getPassword();
            String cloudUrl = cloudLoginDetails.getUrl();

            String soapUrl = cloudUrl + erpurl;
            log.info("SOAP URl: "+soapUrl);
            ErpIntegrationServiceStub erpIntegrationServiceStub = new ErpIntegrationServiceStub(soapUrl);

            ServiceClient client = erpIntegrationServiceStub._getServiceClient();

            // BasicAuthentication for soap service
            HttpTransportPropertiesImpl.Authenticator auth = new HttpTransportPropertiesImpl.Authenticator();
            auth.setUsername(userName);
            auth.setPassword(password);
            client.getOptions().setProperty(org.apache.axis2.transport.http.HTTPConstants.AUTHENTICATE, auth);
            client.getOptions().setProperty(org.apache.axis2.transport.http.HTTPConstants.CHUNKED, false);

            DownloadESSJobExecutionDetailsResponse downloadESSJobExecutionDetailsResponse = erpIntegrationServiceStub
                    .downloadESSJobExecutionDetails(downloadESSJobExecutionDetails);

            DocumentDetails[] documentDetails = downloadESSJobExecutionDetailsResponse.getResult();

            log.info("DocumentDetails size: " + documentDetails.length);
            DocumentDetails details = documentDetails[0];

            DataHandler dataHandler = details.getContent().getBase64BinaryDataHandler();
            log.info("dataHandler::::" + dataHandler);
            buf = dataHandler.getDataSource().getInputStream().readAllBytes();

            log.info("Saving cloud log file to DB");
            CrCloudLogDetails crCloudLogDetails = new CrCloudLogDetails();
            crCloudLogDetails.setLoadRequestId(Long.valueOf(requestId));
            crCloudLogDetails.setLogFileZip(buf);
            crCloudLogFileRepo.save(crCloudLogDetails);

        } catch (Exception e) {
            log.error("Error occurred during downloadESSJobExecutionDetails for requestId :"+requestId+". "+e.getMessage());
            throw new Exception(e.getMessage());
        }
        return buf;
    }

    private CloudLoginDetails getCloudLoginDetails(Long podId, String bearerToken) throws Exception {
        HttpHeaders header = new HttpHeaders();
        header.setAccept(Arrays.asList(MediaType.APPLICATION_JSON));
        header.set("Authorization", bearerToken);
        HttpEntity<String> entity = new HttpEntity<String>(header);
        RestTemplate restTemplate = new RestTemplate();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root;
        String cloudUrl;
        CloudLoginDetails cloudLoginDetails = new CloudLoginDetails();
        try {
            String url = convertriteAdminHost + "/api/convertriteadmin/podcloudconfigs/" + podId;
            ResponseEntity<String> objects = restTemplate.exchange(url, HttpMethod.GET, entity, String.class);
            root = mapper.readTree(objects.getBody());
            if (root.has("payload") && root.get("payload").isArray() && !root.get("payload").isEmpty()) {
                JsonNode payloadNode = root.get("payload").get(0);
                cloudUrl = payloadNode.get("url").asText();
                String userName = payloadNode.get("modules").get(0).get("userName").asText();
                String password = payloadNode.get("modules").get(0).get("password").asText();
                cloudLoginDetails.setUrl(cloudUrl);
                cloudLoginDetails.setUsername(userName);
                cloudLoginDetails.setPassword(password);
            } else {
                log.info("No Cloud Config information found for Pod Id :"+podId);
            }
            return cloudLoginDetails;

        } catch (Exception e) {
            log.error("Exception while trying to get Pod Cloud Config for pod id :"+podId+". "+e.getMessage());
            throw new Exception(e.getMessage());
        }
    }
}