package com.rite.products.convertrite.service;

import org.springframework.web.multipart.MultipartFile;

public interface FileUploadService {

	String uploadFile(MultipartFile file) throws Exception;
}
