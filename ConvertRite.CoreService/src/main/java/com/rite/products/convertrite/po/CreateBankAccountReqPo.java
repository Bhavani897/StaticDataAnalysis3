package com.rite.products.convertrite.po;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Data;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
//@JsonSerialize(using = CreateBankAccountReqPoSerializer.class)
@Data
public class CreateBankAccountReqPo {
            @JsonProperty("AccountHolderName")
    private String accountHolderName;
        @JsonProperty("AccountType")
        private String AccountType;

        @JsonProperty("ApUseAllowedFlag")
        private String ApUseAllowedFlag;

        @JsonProperty("ArUseAllowedFlag")
        private String ArUseAllowedFlag;

        @JsonProperty("BankAccountName")
        private String BankAccountName;

        @JsonProperty("BankAccountNumber")
        private String BankAccountNumber;

        @JsonProperty("BankBranchName")
        private String BankBranchName;

        @JsonProperty("BankName")
        private String BankName;

        @JsonProperty("CountryName")
        private String CountryName;

        @JsonProperty("CurrencyCode")
        private String CurrencyCode;

        @JsonProperty("LegalEntityName")
        private String LegalEntityName;

//        @JsonProperty("MaskedAccountNumber")
//        private String MaskedAccountNumber;

        @JsonProperty("MultiCurrencyAllowedFlag")
        private String MultiCurrencyAllowedFlag;

        @JsonProperty("NettingAccountFlag")
        private String NettingAccountFlag;

        @JsonProperty("PayUseAllowedFlag")
        private String PayUseAllowedFlag;

        @JsonProperty("ZeroAmountAllowed")
        private String ZeroAmountAllowed;



}
//class CreateBankAccountReqPoSerializer extends JsonSerializer<CreateBankAccountReqPo> {
//        @Override
//        public void serialize(CreateBankAccountReqPo value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
//                gen.writeStartObject();
//                Field[] fields = value.getClass().getDeclaredFields();
//                for (Field field : fields) {
//                        try {
//                                field.setAccessible(true);
//                                String fieldName = field.getName();
//                                String capitalizedFieldName = capitalizeFirstLetter(fieldName);
//                                Object fieldValue = field.get(value);
//                                gen.writeObjectField(capitalizedFieldName, fieldValue);
//                        } catch (IllegalAccessException e) {
//                                throw new IOException("Error serializing CreateBankAccountReqPo", e);
//                        }
//                }
//                gen.writeEndObject();
//        }
//
//        private String capitalizeFirstLetter(String str) {
//                if (str == null || str.isEmpty()) {
//                        return str;
//                }
//                return str.substring(0, 1).toUpperCase() + str.substring(1);
//        }
//}