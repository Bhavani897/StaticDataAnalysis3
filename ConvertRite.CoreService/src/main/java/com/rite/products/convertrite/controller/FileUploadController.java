package com.rite.products.convertrite.controller;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.rite.products.convertrite.exception.ConvertRiteException;
import com.rite.products.convertrite.model.XxrFileDataLoad;
import com.rite.products.convertrite.po.UploadFileResponse;
import com.rite.products.convertrite.respository.XxrFileDataLoadRepository;
import com.rite.products.convertrite.service.FileUploadService;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController
public class FileUploadController {

	private static final Logger log = LoggerFactory.getLogger(FileUploadController.class);
	
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
	XxrFileDataLoadRepository xxrFileDataLoadRepository;
	@Autowired
	FileUploadService fileUploadService;

	@ApiOperation(value = "This Api is for uploading file")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@PostMapping("/uploadFile1")
	public ResponseEntity<UploadFileResponse> uploadFile1(@RequestParam("file") MultipartFile file)
			throws ConvertRiteException {
		log.info("Start of uploadFile Method in Controller######");
		String fileName = "";

		try {
			fileName = fileUploadService.uploadFile(file);
		} catch (Exception e) {
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return new ResponseEntity<UploadFileResponse>(new UploadFileResponse(fileName,

				file.getContentType(), file.getSize()), new HttpHeaders(), HttpStatus.OK);

	}

	@SuppressWarnings({ "rawtypes" })
	@ApiOperation(value = "This Api is for downloading file")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/downloadtemplate")
	public ResponseEntity downloadFile(@RequestParam("objectCode") String objectCode,
			@RequestParam("metaDataFileType") String metaDataFileType) throws ConvertRiteException {
		log.info("Start of downloadFile Method in Controller######");

		// response.setContentType("text/csv");

		try {
			if (("TABLE").equalsIgnoreCase(metaDataFileType)) {
				File file = new File(
						getClass().getClassLoader().getResource("Rite_Genarate_Table_File_040820.csv").getFile());

				// response.setHeader("Content-Disposition", "attachment; file=\"" +
				// file.getName() + "\"");

				CSVReader reader = new CSVReader(new FileReader(file));
				List<String[]> csvBody = reader.readAll();
				// get CSV row column and replace with by using row and column
				for (int i = 1; i < csvBody.size(); i++) {

					csvBody.get(i)[0] = objectCode; // Target replacement

				}

				reader.close();

				// Write to CSV file which is open
				CSVWriter writer = new CSVWriter(new FileWriter(file));

				// response.getWriter().write(csvBody.toString());

				writer.writeAll(csvBody);
				writer.flush();
				writer.close();
				log.info(file.getName());
				return ResponseEntity.ok().header("Content-Disposition", "attachment; filename=" + file.getName())
						.contentLength(file.length()).contentType(MediaType.parseMediaType("text/csv"))
						.body(new FileSystemResource(file));

			} else if (("COLUMN").equalsIgnoreCase(metaDataFileType)) {
				/*
				 * File file = new File( getClass().getClassLoader().getResource(
				 * "Rite_Source_Column_Template_040820.csv").getFile());
				 */
			} else {
				throw new ConvertRiteException("metaDataFileType Value Should be TABLE OR COLUMN",
						HttpStatus.BAD_REQUEST);
			}
		} catch (Exception e) {
			throw new ConvertRiteException(
					"Please contact System Administrator there is an error while processing the request",
					HttpStatus.INTERNAL_SERVER_ERROR);
		}

		return ResponseEntity.ok().body("ss");

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

	@ApiOperation(value = "This Api is for load data")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/loaddatapoc")
	public String loadData() throws ConvertRiteException, IOException, JSchException, SftpException {
		log.info("Start of loadData Method in Controller######");
		String line = "";
		String header = "";
		StringBuffer sb = new StringBuffer("INSERT ALL");
		sb.append("\n");

//		File file = new File(
//				getClass().getClassLoader().getResource("export1.csv").getFile());

		/*
		 * ChannelSftp channelSftp = setupJsch(); channelSftp.connect();
		 * channelSftp.cd(remoteDir); InputStream in =channelSftp.get("export.csv");
		 * 
		 * BufferedReader br = new BufferedReader(new InputStreamReader(in)); header =
		 * br.readLine(); while ((line = br.readLine()) != null) // returns a Boolean
		 * value { sb.append("   INTO XXR_SUP_SITES_1001(" + header + ") VALUES (" +
		 * line + ")"); }
		 * 
		 * sb.append("\n"); sb.append("SELECT * FROM dual;"); br.close();
		 */

		return sb.toString();

	}
	
	@ApiOperation(value = "This Api is for load data")
	@ApiResponses(value = { @ApiResponse(code = 200, message = "Successful Response"),
			@ApiResponse(code = 500, message = "Server Side Error") })
	@GetMapping("/convertclobtocsv")
	public void convertClobToCsv(HttpServletRequest request,HttpServletResponse response) throws IOException {
		log.info("Start of convertClobToCsv Method in Controller######");
	
		XxrFileDataLoad xxrFileDataLoad=xxrFileDataLoadRepository.findAll().stream().filter(x->x.getTemplateName().equalsIgnoreCase("Address Src 1211")).findFirst().get();
		
		String dataLob=xxrFileDataLoad.getDataLob();
		
		//System.out.println(dataLob);
		response.setHeader(HttpHeaders.CONTENT_DISPOSITION,
				"attachment; filename=clob.csv");
		PrintWriter writer=response.getWriter();
		CSVWriter csvWriter = new CSVWriter(writer, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.NO_QUOTE_CHARACTER,
				CSVWriter.NO_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);

		//System.out.println("RequestId ########"+request.getParameter("requestId"));
		
		csvWriter.writeNext(dataLob.split(","));
		
		csvWriter.flush();
		csvWriter.close();
		
	}

}
