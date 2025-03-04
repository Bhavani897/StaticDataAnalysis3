package com.rite.products.convertrite.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

@Service
public class FileUploadServiceImpl implements FileUploadService {

	private static final Logger log = LoggerFactory.getLogger(FileUploadServiceImpl.class);
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

	public String uploadFile(MultipartFile file) throws Exception {
		log.info("Start of uploadFile Method in Service class######");

		String fileName = StringUtils.cleanPath(file.getOriginalFilename());

		/*
		 * ChannelSftp channelSftp = setupJsch(); channelSftp.connect();
		 * channelSftp.cd(remoteDir);
		 * 
		 * 
		 * 
		 * Path targetLocation =
		 * Paths.get(localDir).toAbsolutePath().normalize().resolve(fileName);
		 * Files.copy(file.getInputStream(), targetLocation,
		 * StandardCopyOption.REPLACE_EXISTING); String localFile =
		 * localDir+"\\sample.csv";
		 * 
		 * 
		 * 
		 * //channelSftp.put(localFile, remoteDir + "sample.csv");
		 * channelSftp.put(file.getInputStream(),fileName); channelSftp.exit();
		 */

		// Normalize file name

		/*
		 * try {
		 * 
		 * Path targetLocation =
		 * Paths.get(uploadDir).toAbsolutePath().normalize().resolve(fileName);
		 * Files.copy(file.getInputStream(), targetLocation,
		 * StandardCopyOption.REPLACE_EXISTING);
		 * 
		 * } catch (Exception e) { // TODO Auto-generated catch block throw new
		 * Exception(e.getMessage()); }
		 */

		return fileName;
	}

	/*
	 * private ChannelSftp setupJsch() throws JSchException {
	 * 
	 * JSch jsch = new JSch();
	 * 
	 * Session jschSession = jsch.getSession(username, remoteHost);
	 * jschSession.setPassword(password); // jschSession.setPort(port);
	 * java.util.Properties config = new java.util.Properties();
	 * config.put("StrictHostKeyChecking", "no"); jschSession.setConfig(config);
	 * jschSession.connect(); return (ChannelSftp) jschSession.openChannel("sftp");
	 * }
	 */

}
