package com.rite.products.convertrite.service;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.servlet.http.HttpServletResponse;
import javax.validation.ValidationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;

import com.opencsv.CSVWriter;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.model.XxrBlobConvertrite;
import com.rite.products.convertrite.model.XxrCloudDataProcess;
import com.rite.products.convertrite.model.XxrCloudDataProcessView;
import com.rite.products.convertrite.po.CloudDataProcessPagReqPo;
import com.rite.products.convertrite.po.CloudDataProcessingReqPo;
import com.rite.products.convertrite.respository.XxrBlobConvertriteRepository;
import com.rite.products.convertrite.respository.XxrCloudDataProcessRepository;
import com.rite.products.convertrite.respository.XxrCloudDataProcessViewRepository;

@Service
public class CloudDataProcessingServiceImpl implements CloudDataProcessingService {

	private static final Logger log = LoggerFactory.getLogger(CloudDataProcessingServiceImpl.class);

	// private static AtomicLong numberGenerator = new AtomicLong(1);

	@Value("${file.local-dir}")
	private String localDir;

	@Value("${file.upload-dir}")
	private String remoteDir;

	@Value("${sftp.client.host}")
	private String remoteHost;

	@Value("${sftp.client.username}")
	private String username;

	/*
	 * @Value("${sftp.client.password}") private String password;
	 */
	@Autowired
	XxrCloudDataProcessRepository xxrCloudDataProcessRepository;
	@Autowired
	XxrBlobConvertriteRepository xxBlobConvertriteRepository;
	@Autowired
	XxrCloudDataProcessViewRepository xxrCloudDataProcessViewRepository;

	@Override
	public XxrCloudDataProcess cloudDataProcessingRequests(CloudDataProcessingReqPo cloudDataProcessingReqPo)
			throws Exception, BadRequestException {
		log.info("Start of cloudDataProcessingRequests Method::::::::");

		XxrCloudDataProcess xxrCloudDataProcess = new XxrCloudDataProcess();
		XxrCloudDataProcess xxrCloudDataProcessRes = new XxrCloudDataProcess();
		String reqId = null;

		// XxrCloudDataProcess xxrCloudDataProces = xxrCloudDataProcessRepository.findByrequestId(cloudDataProcessingReqPo.getRequestId());

		if (cloudDataProcessingReqPo.getRequestId() == null) {
			UUID requestId = UUID.randomUUID();
			reqId = requestId.toString();
			xxrCloudDataProcess.setRequestId(reqId);
			xxrCloudDataProcess.setDescription(cloudDataProcessingReqPo.getDescription());
			xxrCloudDataProcess.setSqlQuery(cloudDataProcessingReqPo.getSqlQuery());
			xxrCloudDataProcess.setPodId(cloudDataProcessingReqPo.getPodId());
			xxrCloudDataProcess.setProjectId(cloudDataProcessingReqPo.getProjectId());
			xxrCloudDataProcess.setLookupFlag(cloudDataProcessingReqPo.getLookUpFlag());
			xxrCloudDataProcess.setStatus("Starting");
			java.util.Date date = new java.util.Date();
			//log.info("date::::::" + date);
			xxrCloudDataProcess.setCreationDate(date);
			xxrCloudDataProcess.setScheduledJobCall(cloudDataProcessingReqPo.getScheduledJobCall());
			if (cloudDataProcessingReqPo.getPriority() != null)
				xxrCloudDataProcess.setPriority(cloudDataProcessingReqPo.getPriority());
			else
				xxrCloudDataProcess.setPriority(0);
			if (("Y").equalsIgnoreCase(cloudDataProcessingReqPo.getExtractionFlag())) {
				String tableNameExist = xxrCloudDataProcessRepository.findByTableNameIgnoreCase(cloudDataProcessingReqPo.getTableName());
				if (tableNameExist != null) {
					throw new BadRequestException("Table Name is already exist");
				} else {
					xxrCloudDataProcess.setExtractionFlag(cloudDataProcessingReqPo.getExtractionFlag());
					xxrCloudDataProcess.setTableName(cloudDataProcessingReqPo.getTableName());
					xxrCloudDataProcess.setMetaData(cloudDataProcessingReqPo.getMetaData());
				}
			}
		} else {
			Optional<XxrCloudDataProcess> xxrCloudDataProcessOptional = xxrCloudDataProcessRepository.findByRequestId(cloudDataProcessingReqPo.getRequestId());
			if (xxrCloudDataProcessOptional.isPresent()) {
				xxrCloudDataProcess = xxrCloudDataProcessOptional.get();
				xxrCloudDataProcess.setDescription(cloudDataProcessingReqPo.getDescription());
				xxrCloudDataProcess.setSqlQuery(cloudDataProcessingReqPo.getSqlQuery());
				xxrCloudDataProcess.setStatus("Starting");
				java.util.Date date = new java.util.Date();
				xxrCloudDataProcess.setLastUpdatedDate(date);
				xxrCloudDataProcess.setMetaData(cloudDataProcessingReqPo.getMetaData());
				xxrCloudDataProcess.setTableName(cloudDataProcessingReqPo.getTableName());
			}

		}
		xxrCloudDataProcessRes = xxrCloudDataProcessRepository.save(xxrCloudDataProcess);
		return xxrCloudDataProcessRes;
	}

	@Override
	public List<XxrCloudDataProcessView> getAllCloudDataRequests(CloudDataProcessPagReqPo cloudDataProcessPagReqPo,HttpHeaders httpHeaders)
			throws Exception {
		log.info("Start of getAllCloudDataRequests Method::::::::");
		List<XxrCloudDataProcessView> xxrCloudDataProcess = new ArrayList<>();
		Page<XxrCloudDataProcessView> page = null;
		try {
			Pageable pageable = PageRequest.of(cloudDataProcessPagReqPo.getPageNo(),
					cloudDataProcessPagReqPo.getPageSize(),
					Sort.by(cloudDataProcessPagReqPo.getSortDirection(), cloudDataProcessPagReqPo.getSortBy()));
			if (cloudDataProcessPagReqPo.getExtractionFlag() == null) {
				page = xxrCloudDataProcessViewRepository.findAll(pageable);
			} else
				page = xxrCloudDataProcessViewRepository.findAllByExtractionFlag("Y", pageable);

			httpHeaders.set("pagecount", String.valueOf(page.getTotalPages()));
			httpHeaders.set("totalcount", String.valueOf(page.getTotalElements()));
			if (page.hasContent())
				xxrCloudDataProcess = page.getContent();
			// xxrCloudDataProcess = xxrCloudDataProcessRepository.findAll();
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
		return xxrCloudDataProcess;
	}

	@Override
	public void downloadCsvFile(String requestId, HttpServletResponse response) throws Exception {
		log.info("Start of downloadCsvFile Method in Service class######");
		XxrBlobConvertrite res = new XxrBlobConvertrite();
		try {
			PrintWriter writer = response.getWriter();
			Optional<XxrBlobConvertrite> lobResp = xxBlobConvertriteRepository.findById(requestId);
			if (lobResp.isPresent()) {
				res = lobResp.get();
				CSVWriter csvWriter = new CSVWriter(writer, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER,
						CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
				String lob = res.getBlob();
				String[] lineArr = lob.split("\n");
				List<String[]> csvList = new ArrayList<>();
				for (String line : lineArr) {
					csvList.add(line.split(","));
				}

				csvWriter.writeAll(csvList);
				csvWriter.flush();
				csvWriter.close();
			}
			// channelSftp.exit();
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
	}


	@Override
	public void deleteAdhocData(String requestId) throws Exception {
		Optional<XxrCloudDataProcess> desktopModelOptional = xxrCloudDataProcessRepository.findById(requestId);
		if (desktopModelOptional.isEmpty())
			throw new ValidationException("There is no data with this requestId");
		xxrCloudDataProcessRepository.deleteById(requestId);
	}

	/*
	 * private ChannelSftp setupJsch() throws JSchException { JSch jsch = new
	 * JSch(); Session jschSession = jsch.getSession(username, remoteHost);
	 * jschSession.setPassword(password); // jschSession.setPort(port);
	 * java.util.Properties config = new java.util.Properties();
	 * config.put("StrictHostKeyChecking", "no"); jschSession.setConfig(config);
	 * jschSession.setTimeout(6000); jschSession.connect(); return (ChannelSftp)
	 * jschSession.openChannel("sftp"); }
	 */
}
