package com.rite.products.convertrite.service;

import com.rite.products.convertrite.Validations.Validations;
import com.rite.products.convertrite.exception.BadRequestException;
import com.rite.products.convertrite.exception.ValidationException;
import com.rite.products.convertrite.model.CrFbdiCols;
import com.rite.products.convertrite.model.CrFbdiHdrs;
import com.rite.products.convertrite.model.CrFbdiView;
import com.rite.products.convertrite.po.*;
import com.rite.products.convertrite.respository.CrFBDIViewRepo;
import com.rite.products.convertrite.respository.CrFbdiColsRepo;
import com.rite.products.convertrite.respository.CrFbdiHdrsRepo;
import com.rite.products.convertrite.utils.ControlFileParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.util.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Service
@Slf4j
public class CrFbdiServiceImpl implements CrFbdiService {

    @Autowired
    CrFbdiHdrsRepo crFbdiHdrsRepo;
    
    @Autowired
    CrFBDIViewRepo crFBDIViewRepo;
    
    @Autowired
    CrFbdiColsRepo crFbdiColsRepo;

    @Value("${firstpartof-ctl-download-url}")
    private String ctlUrlFirstPart;
    @Value("${secondpartof-ctl-download-url}")
    private String ctlUrlSecondPart;

    @Override
    public SaveFbdiHdrsPo saveFbdiTemplateHdrs(SaveFbdiTempHdrsPo fbdiTemplateHeaderPo, HttpServletRequest request) throws Exception {
        java.util.Date date = null;
        CrFbdiHdrs crFbdiHdrs = new CrFbdiHdrs();
        CrFbdiHdrs crFbdiHdrsRes = new CrFbdiHdrs();
        SaveFbdiHdrsPo saveFbdiHdrsPo = new SaveFbdiHdrsPo();
        if (fbdiTemplateHeaderPo.getFbdiTemplateId() == null) {
            Long crFbdiHdrsCheck = crFbdiHdrsRepo
                    .getFbdiTemplateId(fbdiTemplateHeaderPo.getFbdiTemplateName(),
                            fbdiTemplateHeaderPo.getVersion(), fbdiTemplateHeaderPo.getSheetName(), fbdiTemplateHeaderPo.getObjectId());
            if (crFbdiHdrsCheck != null)
                throw new ValidationException("Fbdi Template Already Exists.");
            crFbdiHdrs.setFbdiTemplateName(fbdiTemplateHeaderPo.getFbdiTemplateName());
            crFbdiHdrs.setApi(fbdiTemplateHeaderPo.getApi());
            crFbdiHdrs.setObjectId(fbdiTemplateHeaderPo.getObjectId());
            crFbdiHdrs.setProjectId(fbdiTemplateHeaderPo.getProjectId());
            crFbdiHdrs.setParentObjectId(fbdiTemplateHeaderPo.getParentObjectId());
            crFbdiHdrs.setVersion(fbdiTemplateHeaderPo.getVersion());
            crFbdiHdrs.setSheetName(fbdiTemplateHeaderPo.getSheetName());
            date = new java.util.Date();
            crFbdiHdrs.setCreatedBy(fbdiTemplateHeaderPo.getCreatedBy());
            crFbdiHdrs.setCreationDate(date);
            crFbdiHdrs.setLastUpdatedDate(date);
            crFbdiHdrs.setLastUpdatedBy(fbdiTemplateHeaderPo.getUpdatedBy());
        } else {
            Optional<CrFbdiHdrs> crFbdiHdrsOptional = crFbdiHdrsRepo.findById(fbdiTemplateHeaderPo.getFbdiTemplateId());
            if (crFbdiHdrsOptional.isPresent()) {
                crFbdiHdrs = crFbdiHdrsOptional.get();
                crFbdiHdrs.setFbdiTemplateId(fbdiTemplateHeaderPo.getFbdiTemplateId());
                crFbdiHdrs.setFbdiTemplateName(fbdiTemplateHeaderPo.getFbdiTemplateName());
                crFbdiHdrs.setApi(fbdiTemplateHeaderPo.getApi());
                crFbdiHdrs.setObjectId(fbdiTemplateHeaderPo.getObjectId());
                crFbdiHdrs.setProjectId(fbdiTemplateHeaderPo.getProjectId());
                crFbdiHdrs.setParentObjectId(fbdiTemplateHeaderPo.getParentObjectId());
                crFbdiHdrs.setVersion(fbdiTemplateHeaderPo.getVersion());
                crFbdiHdrs.setSheetName(fbdiTemplateHeaderPo.getSheetName());
                date = new java.util.Date();
                crFbdiHdrs.setLastUpdatedDate(date);
                crFbdiHdrs.setLastUpdatedBy(crFbdiHdrs.getLastUpdatedBy());
            } else
                throw new Exception("There is no FbdiTemplate with this TemplateId");
        }
        crFbdiHdrsRes = crFbdiHdrsRepo.save(crFbdiHdrs);
        saveFbdiHdrsPo.setCrFbdiHdrs(crFbdiHdrsRes);
        saveFbdiHdrsPo.setMessage("Succesfully Saved Fbdi Template Hdrs");
        return saveFbdiHdrsPo;

    }

     @Override
    public List<FbdiColumnSequencePo> getFbdiColumnSequence(String fileName, String version, HttpServletResponse resp)
            throws Exception {
        log.info("Start of getFbdiColumnSequence in service ###");
        List<FbdiColumnSequencePo> columnSequencePo = new ArrayList<>();
        List<String> columnNames = new ArrayList<>();
        try {
            Path path = downloadFbdiTemplateFromServer(fileName, version, resp);

            ControlFileParser ctlFileParser = new ControlFileParser(path.toString() + File.separator + fileName);
            int sequence = 10;
            columnNames = ctlFileParser.getColumnNames();
            for (String columnName : columnNames) {
                if(!columnName.equalsIgnoreCase("when") && !columnName.equalsIgnoreCase("else")){
                    FbdiColumnSequencePo fbdiColumnSequencePo = new FbdiColumnSequencePo();
                    fbdiColumnSequencePo.setDatabaseColumn(columnName);
                    fbdiColumnSequencePo.setSequence(sequence);
                    columnSequencePo.add(fbdiColumnSequencePo);
                    sequence += 10;
                }
            }

        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return columnSequencePo;
    }

    private Path downloadFbdiTemplateFromServer(String fileName, String version, HttpServletResponse resp)
            throws Exception {
        log.info("Start of downloadFbdiTemplateFromServer Method ###");
        String url = "";
        Path target = null;
        try {
            String sb = ctlUrlFirstPart + version +
                    ctlUrlSecondPart +
                    fileName;
            url = sb;
            resp.setHeader("API", url);
            // create Temp Directory
            target = Files.createTempDirectory("");
            log.info("target:::::" + target);
            // website url
            URL website = new URL(url);
            File file = new File(target + File.separator + fileName);
            try (InputStream in = website.openStream();
                 OutputStream outputStream = new FileOutputStream(file)) {
                IOUtils.copy(in, outputStream);
            } catch (IOException e) {
               // e.printStackTrace();
                throw new Exception(e.getMessage());
            }
        } catch (FileNotFoundException e) {
            // e.printStackTrace();
            throw new Exception(e.getMessage());
        } catch (Exception e) {
             //e.printStackTrace();
            throw new Exception(e.getMessage());
        }
        return target;
    }


    @Override
    public CrSaveFbdiTempColsResPo saveFbdiTemplateColumns(
            List<SaveFbdiTemplateColumnsPo> fbdiTemplateColumnsPo, HttpServletRequest request)
            throws BadRequestException {
        log.info("Start of saveFbdiTemplateColumnsWithJpa #######");
        SaveFbdiTemplateColumnsResPo saveFbdiTemplateColumnsResPo = new SaveFbdiTemplateColumnsResPo();
        List<CrFbdiCols> crFbdiColsList = new ArrayList<>();
        List<CrFbdiCols> fbdiTempColsResLi = new ArrayList<>();
        CrSaveFbdiTempColsResPo crSaveFbdiTempColsResPo = new CrSaveFbdiTempColsResPo();

        for (SaveFbdiTemplateColumnsPo saveFbdiTemplateColumnsPo : fbdiTemplateColumnsPo) {

            if (Validations.isNullOrEmpty(saveFbdiTemplateColumnsPo.getDatabaseColumn())
                    || saveFbdiTemplateColumnsPo.getFbdiTemplateId() == null)
                throw new BadRequestException("fbdiTemplateId and columnName are Mandatory fields");
            if (saveFbdiTemplateColumnsPo.getFbdiColumnId() == null) {
                CrFbdiCols crFbdiCols = new CrFbdiCols();
                crFbdiCols.setFbdiTemplateId(saveFbdiTemplateColumnsPo.getFbdiTemplateId());
                crFbdiCols.setDatabaseColumn(saveFbdiTemplateColumnsPo.getDatabaseColumn());
                crFbdiCols.setFbdiColumnName(saveFbdiTemplateColumnsPo.getFbdiColumnName());
                crFbdiCols.setDatabaseTable(saveFbdiTemplateColumnsPo.getDatabaseTable());
                crFbdiCols.setRequired(saveFbdiTemplateColumnsPo.getRequired());
                crFbdiCols.setSequence(saveFbdiTemplateColumnsPo.getSequence());
                crFbdiCols.setCreatedBy("ConvertRite");
                crFbdiCols.setCreationDate(new java.sql.Date(new java.util.Date().getTime()));
                crFbdiCols.setLastUpdatedBy("ConvertRite");
                crFbdiCols.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
                crFbdiColsList.add(crFbdiCols);
            } else {
                CrFbdiCols crFbdiCols = crFbdiColsRepo.findByFbdiTemplateIdAndFbdiColumnId(
                        saveFbdiTemplateColumnsPo.getFbdiTemplateId(), saveFbdiTemplateColumnsPo.getFbdiColumnId());
                crFbdiCols.setFbdiTemplateId(saveFbdiTemplateColumnsPo.getFbdiTemplateId());

                crFbdiCols.setDatabaseColumn(saveFbdiTemplateColumnsPo.getDatabaseColumn());
                crFbdiCols.setFbdiColumnName(saveFbdiTemplateColumnsPo.getFbdiColumnName());
                crFbdiCols.setDatabaseTable(saveFbdiTemplateColumnsPo.getDatabaseTable());
                crFbdiCols.setRequired(saveFbdiTemplateColumnsPo.getRequired());
                crFbdiCols.setSequence(saveFbdiTemplateColumnsPo.getSequence());
                crFbdiCols.setLastUpdatedBy("ConvertRite");
                crFbdiCols.setLastUpdatedDate(new java.sql.Date(new java.util.Date().getTime()));
                crFbdiColsList.add(crFbdiCols);
            }
        }

        fbdiTempColsResLi = crFbdiColsRepo.saveAll(crFbdiColsList);
        crSaveFbdiTempColsResPo.setFbdiTemplateColumns(fbdiTempColsResLi);
        crSaveFbdiTempColsResPo.setMessage("Successfully Saved/Updated FbdiTemplateColumns");
        return crSaveFbdiTempColsResPo;
    }

    @Override
    public List<CrFbdiHdrs>  getFbdiTemplates() throws Exception {
        log.info("Start of getFbdiTemplates in Service ####");
        List<CrFbdiHdrs> crFbdiHdrs = new ArrayList<>();
        crFbdiHdrs = crFbdiHdrsRepo.findAll();
        return crFbdiHdrs;
    }
    @Override
    public List<CrFbdiCols> getFbdiTemplateColumns(Long fbdiTemplateId) throws Exception {
        log.info("Start of getFbdiTemplateColumns Method in Service ######");
        List<CrFbdiCols> crFbdiColsList = new ArrayList<>();
        try {
            crFbdiColsList = crFbdiColsRepo.findByfbdiTemplateId(fbdiTemplateId);
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return crFbdiColsList;
    }

    @Override
    public List<FbdiColumnSequencePo> uploadCtlGetColumnSeq(MultipartFile file, String version, Long objectId) {
//        log.info("Start of uploadCtlGetColumnSeq #######");
//        byte[] bytes = null;
//        Path target = null;
//        List<String> columnNames = new ArrayList<>();
//        List<FbdiColumnSequencePo> columnSequencePo = new ArrayList<>();
//        CrFbdiHdrs crFbdiHdrs = crFbdiHdrsRepo.findByObjectIdAndVersion(objectId, version);
//        if (crFbdiHdrs != null) {
//            bytes = IOUtils.toByteArray(file.getInputStream());
//            crFbdiHdrs.setCtlFileBlob(bytes);
//            XxrFbdiTempHdrs xxrFbdiTempHdrRes = xxrFbdiTempHdrsRepository.save(xxrFbdiTempHdrs);
//        }
//        target = Files.createTempDirectory("");
//        log.info("target:::::" + target);
//        String fileName = org.apache.commons.io.FilenameUtils.getName(file.getOriginalFilename());
//        // log.info(file.getName()+"######fileName");
//        File tmpFile = new File(target + File.separator + fileName);
//        try (InputStream in = file.getInputStream(); OutputStream outputStream = new FileOutputStream(tmpFile)) {
//            IOUtils.copy(in, outputStream);
//        } catch (IOException e) {
//            // e.printStackTrace();
//            throw new Exception(e.getMessage());
//        }
//
//        ControlFileParser ctlFileParser = new ControlFileParser(target + File.separator + fileName);
//        int sequence = 10;
//        columnNames = ctlFileParser.getColumnNames();
//        for (String columnName : columnNames) {
////			log.info("columnName######3" + columnName);
//            FbdiColumnSequencePo fbdiColumnSequencePo = new FbdiColumnSequencePo();
//            fbdiColumnSequencePo.setDatabaseColumn(columnName);
//            fbdiColumnSequencePo.setSequence(sequence);
//            columnSequencePo.add(fbdiColumnSequencePo);
//            sequence += 10;
//        }
        return null;
    }

	@Override
	public List<CrFbdiView> getAllFbdITemplates() {
		return crFBDIViewRepo.findAll();
	}
}



