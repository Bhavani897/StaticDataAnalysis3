package com.rite.products.convertrite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;

import com.rite.products.convertrite.model.CloudTableId;
import com.rite.products.convertrite.model.XxrCloudTable;
import com.rite.products.convertrite.model.XxrCloudTemplateColumns;
import com.rite.products.convertrite.model.XxrCloudTemplateHeader;
import com.rite.products.convertrite.po.CloudTemplatePo;
import com.rite.products.convertrite.po.XxrCloudTemplatePo;
import com.rite.products.convertrite.respository.CloudMetaDataRepository;
import com.rite.products.convertrite.respository.CloudTemplateHeaderDaoImpl;
import com.rite.products.convertrite.respository.SourceTemplateColumnsRepository;
import com.rite.products.convertrite.respository.SourceTemplateHeadersRepository;
import com.rite.products.convertrite.respository.XxrCloudColumnsRepository;
import com.rite.products.convertrite.respository.XxrCloudTableRepository;
import com.rite.products.convertrite.respository.XxrCloudTemplateColumnsRepository;
import com.rite.products.convertrite.service.XxrColudServiceImpl;


@SpringBootTest
public class ConvertRiteApiApplicationTests {

	private static final Logger log = LoggerFactory.getLogger(ConvertRiteApiApplicationTests.class);
	@InjectMocks
	private XxrColudServiceImpl xxrColudServiceImpl;
	@Mock
	private XxrCloudTableRepository xxrCloudTableRepository;
	@Mock
	XxrCloudTemplateColumnsRepository xxrCloudTemplateColumnsRepository;
	@Mock
	CloudMetaDataRepository cloudMetaDataRepository;
	@Mock
	SourceTemplateHeadersRepository sourceTemplateHeadersRepository;
	@Mock
	XxrCloudColumnsRepository xxrCloudColumnsRepository;
	@Mock
	SourceTemplateColumnsRepository sourceTemplateColumnsRepository;
	@Mock
	CloudTemplateHeaderDaoImpl cloudTemplateHeaderDaoImpl;

	@Test
	public void contextLoads() {

	}

	@Test
	public void getAllCloudData() {
		List<XxrCloudTable> list = new ArrayList<XxrCloudTable>();
		XxrCloudTemplatePo cloudTemplatePo=new XxrCloudTemplatePo();
		XxrCloudTable obj1 = new XxrCloudTable();
		CloudTableId cloudTableId = new CloudTableId();
		cloudTableId.setTableId(5233);
		cloudTableId.setTableName("POZ_SUPPLIERS_INT");
		obj1.setCloudTableId(cloudTableId);
		/* obj1.setUserTableName("POZ_SUPPLIERS_INT"); */
		list.add(obj1);
		
		String [] values= {"SUPPLIER","SUPPLIER SITE"};
		String[] pod= {"1000","100","10"};
		String[] bu= {"1","2","3"};
		String [] templateHeaders= {"SUPPLIER TEMPLATE","TEST JOHN TEMPLATE"};
		when(xxrCloudTableRepository.findAll()).thenReturn(list);
		
		/*
		 * when(cloudMetaDataRepository.getValues("OBJECT_NAME")).thenReturn(values);
		 * when(cloudMetaDataRepository.getValues("POD")).thenReturn(pod);
		 * when(cloudMetaDataRepository.getValues("BU")).thenReturn(bu);
		 */
		/*
		 * when(sourceTemplateHeadersRepository.getTemplateHeaders(values)).thenReturn(
		 * templateHeaders);
		 */
		
		cloudTemplatePo.setCloudTableMetaData(list);
		cloudTemplatePo.setBu(bu);
		cloudTemplatePo.setPod(pod);
		cloudTemplatePo.setObjectCodes(values);
		cloudTemplatePo.setParentObjectCode(values);
		cloudTemplatePo.setTemplateHeaders(templateHeaders);	
		try {
			cloudTemplatePo= xxrColudServiceImpl.getAllCloudData();
		}
		catch(Exception e) {
			 assertThat(e.getMessage().equals("Name must not be null"));
		}	
	}
	
	@Test
	public void getCloudSourceColumns() throws Exception {
		
		//CloudSourceColumnsPo cloudSourceColumnsPo = new CloudSourceColumnsPo();
		
		long templateId=1;
		long tableId=5233;
		String[] cloudColumns= {"ACCTS_PAY_CODE_COMBINATION_ID","ALLOW_AWT_FLAG"};
		//String[] sourceColumns= {"IBY_BANK_CHARGE_BEARER","REMIT_ADVICE_DELIVERY_METHOD"};

		try {
			when(xxrCloudTableRepository.getTableId("POZ_SUPPLIERS_INT")).thenReturn(tableId);
			when(sourceTemplateHeadersRepository.getTemplateId("SUPPLIER TEMPLATE")).thenReturn(templateId);	
			log.info("tableId:::::: " + tableId + " templateId:::: " + templateId);
			when(xxrCloudColumnsRepository.getColumnName(tableId)).thenReturn(cloudColumns);
			//when(sourceTemplateColumnsRepository.getColumnNames(templateId)).thenReturn(sourceColumns);
			//cloudSourceColumnsPo.setCloudColumns(cloudColumns);
			//cloudSourceColumnsPo.setSourceColumns(sourceColumns);
			
			//cloudSourceColumnsPo=xxrColudServiceImpl.getCloudSourceColumns("SUPPLIER TEMPLATE", "POZ_SUPPLIERS_INT");

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
	}
	
	/*
	 * @Test public void getCloudSourceColumnsByIds() throws Exception{
	 * 
	 * CloudSourceColumnsPo cloudSourceColumnsPo = new CloudSourceColumnsPo();
	 * String[] cloudColumns= {"ACCTS_PAY_CODE_COMBINATION_ID","ALLOW_AWT_FLAG"};
	 * String[] sourceColumns=
	 * {"IBY_BANK_CHARGE_BEARER","REMIT_ADVICE_DELIVERY_METHOD"};
	 * 
	 * try {
	 * 
	 * when(xxrCloudColumnsRepository.getColumnName(5233)).thenReturn(cloudColumns);
	 * //when(sourceTemplateColumnsRepository.getColumnNames(1)).thenReturn(
	 * sourceColumns); //cloudSourceColumnsPo.setCloudColumns(cloudColumns);
	 * //cloudSourceColumnsPo.setSourceColumns(sourceColumns);
	 * cloudSourceColumnsPo=xxrColudServiceImpl.getCloudSourceColumnsByIds(1l,
	 * 5233l);
	 * 
	 * } catch (Exception e) { throw new Exception(e.getMessage()); }
	 * 
	 * }
	 */

	@Test
	public void getCloudTemplate() throws Exception {
		
		CloudTemplatePo cloudTemplatePo=new CloudTemplatePo();
		/* cloudTemplatePo.setPodId(5); */
		cloudTemplatePo.setObjectCode("SUPPLIER");
		
		XxrCloudTemplateHeader xxrCloudTemplateHeader=new XxrCloudTemplateHeader();
		//xxrCloudTemplateHeader.setBu(5);
		//xxrCloudTemplateHeader.setPodId(5);
		//xxrCloudTemplateHeader.setObjectCode("SUPPLIER");
		//xxrCloudTemplateHeader.setParentObjectCode("SUPPLIER");
		xxrCloudTemplateHeader.setBuSpecific("N");
		xxrCloudTemplateHeader.setTemplateId(102);
		List<XxrCloudTemplateHeader> list = new ArrayList<>();
		list.add(xxrCloudTemplateHeader);
		try {
			when(cloudTemplateHeaderDaoImpl.getCloudTemplate(cloudTemplatePo)).thenReturn(list);
			list=xxrColudServiceImpl.getCloudTemplate(cloudTemplatePo);
			
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}
	}
	

	@Test
	public void getCloudTemplateColumns() throws Exception {
		List<XxrCloudTemplateColumns> cloudTemplateColumnsList = new ArrayList<>();
		
		XxrCloudTemplateColumns xxrCloudTemplateColumns=new XxrCloudTemplateColumns();
		xxrCloudTemplateColumns.setTemplateId(1);
		xxrCloudTemplateColumns.setColumnName("REMIT_ADVICE_DELIVERY_METHOD");
		//xxrCloudTemplateColumns.setSourceColumnName("INVOICE_AMOUNT_LIMIT");
		//xxrCloudTemplateColumns.setStartDate(new Date(30-06-2020));
		//xxrCloudTemplateColumns.setEndDate(new Date(03-07-2020));
		xxrCloudTemplateColumns.setSeq(101);
		xxrCloudTemplateColumns.setMappingType("Constant");
		//xxrCloudTemplateColumns.setMappingValue("hhh");
		//xxrCloudTemplateColumns.setMappingSetName("SET NAME");
		
		XxrCloudTemplateColumns xxrCloudTemplateColumn=new XxrCloudTemplateColumns();
		xxrCloudTemplateColumn.setTemplateId(1);
		xxrCloudTemplateColumn.setColumnName("REMIT_ADVICE_DELIVERY_METHOD1");
		//xxrCloudTemplateColumn.setSourceColumnName("INVOICE_AMOUNT_LIMIT1");
		//xxrCloudTemplateColumn.setStartDate(new Date(30-06-2020));
		//xxrCloudTemplateColumn.setEndDate(new Date(03-07-2020));
		xxrCloudTemplateColumn.setSeq(101);
		xxrCloudTemplateColumn.setMappingType("Constant");
		//xxrCloudTemplateColumn.setMappingValue("mapping");
		//xxrCloudTemplateColumn.setMappingSetName("SET NAME");
		
		cloudTemplateColumnsList.add(xxrCloudTemplateColumns);
		cloudTemplateColumnsList.add(xxrCloudTemplateColumn);
		
		//when(xxrCloudTemplateColumnsRepository.findByTemplateIdOrderByselectedAsc(1)).thenReturn(cloudTemplateColumnsList);
		
		//cloudTemplateColumnsList = xxrColudServiceImpl.getCloudTemplateColumns(1);
		
	}
}
