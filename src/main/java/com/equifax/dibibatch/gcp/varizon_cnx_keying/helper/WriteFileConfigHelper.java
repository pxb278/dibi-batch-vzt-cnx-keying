package com.equifax.dibibatch.gcp.varizon_cnx_keying.helper;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.beam.sdk.values.Row;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.utils.ObjectUtils;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.io.PGPEncryptFileIO;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.io.PGPEncryptFileIO.FieldFn;

public class WriteFileConfigHelper {

	public static Map<String, PGPEncryptFileIO.FieldFn<Row>> getConfigForExtractDICNX0000002EfxVzwFile() {
		Map<String, PGPEncryptFileIO.FieldFn<Row>> config = new LinkedHashMap<>();

		config.put("SURROGATE_KEY", (c) -> c.element().getString("SURROGATE_KEY"));
		config.put("CURR_CNX_ID", (c) -> c.element().getString("CURR_CNX_ID"));
		config.put("CLEAN_SSN", (c) -> c.element().getString("CLEAN_SSN"));
		config.put("CLEAN_DOB", (c) -> c.element().getString("CLEAN_DOB"));
		config.put("NAME", (c) -> c.element().getString("NAME"));
		config.put("ADDR1", (c) -> c.element().getString("ADDR1"));
		config.put("ADDR2", (c) -> (c.element().getString("ADDR2")));
		config.put("CITY", (c) -> c.element().getString("CITY"));
		config.put("STATE", (c) -> c.element().getString("STATE"));
		config.put("ZIP", (c) -> c.element().getString("ZIP"));
		config.put("COUNTRY_CD", (c) -> c.element().getString("COUNTRY_CD"));
		config.put("CLEAN_PHONE", (c) -> c.element().getString("CLEAN_PHONE"));
		config.put("ADDR_ID", (c) -> c.element().getString("ADDR_ID"));
		config.put("FILLER", (c) -> null);

		return config;
	}

	public static Map<String, PGPEncryptFileIO.FieldFn<Row>> getConfigForExtractVzDsFuzzyReqFile() {
		Map<String, PGPEncryptFileIO.FieldFn<Row>> config = new LinkedHashMap<>();

		config.put("SURROGATE_KEY", (c) -> c.element().getString("SURROGATE_KEY"));
		config.put("FIRST_NAME", (c) -> c.element().getString("FIRST_NAME"));
		config.put("MIDDLE_NAME", (c) -> c.element().getString("MIDDLE_NAME"));
		config.put("LAST_NAME", (c) -> c.element().getString("LAST_NAME"));
		config.put("ADDRESS", (c) -> c.element().getString("ADDRESS"));
		config.put("CITY", (c) -> c.element().getString("CITY"));
		config.put("STATE", (c) -> c.element().getString("STATE"));
		config.put("ZIP", (c) -> c.element().getString("ZIP"));

		return config;
	}

	public static Map<String, PGPEncryptFileIO.FieldFn<Row>> getConfigForExtractInactivatedOverrideRecordsFile() {
		Map<String, PGPEncryptFileIO.FieldFn<Row>> config = new LinkedHashMap<>();
		config.put("DATASHARE_ID", (c) -> c.element().getString("DATASHARE_ID"));
		config.put("OLD_BEST_CNX_ID", (c) -> c.element().getString("OLD_BEST_CNX_ID"));
		config.put("EFX_CNX_ID", (c) -> c.element().getString("EFX_CNX_ID"));
		config.put("EFX_HHLD_ID", (c) -> c.element().getString("EFX_HHLD_ID"));
		config.put("EFX_ADDR_ID", (c) -> c.element().getString("EFX_ADDR_ID"));
		config.put("EFX_OVERRIDE_CNX_ID", (c) -> c.element().getString("EFX_OVERRIDE_CNX_ID"));
		config.put("EFX_OVERRIDE_HHLD_ID", (c) -> c.element().getString("EFX_OVERRIDE_HHLD_ID"));

		return config;
	}

	public static Map<String, PGPEncryptFileIO.FieldFn<Row>> getCnxOverrideFile() {
		Map<String, PGPEncryptFileIO.FieldFn<Row>> config = new LinkedHashMap<>();

		config.put("DATASHARE_ID", (c) -> c.element().getString("DATASHARE_ID"));
		config.put("STATUS", (c) -> "Inactive");
		config.put("INDIVIDUALKEY", (c) -> c.element().getString("EFX_OVERRIDE_CNX_ID"));
		config.put("HOUSEHOLDKEY", (c) -> c.element().getString("EFX_OVERRIDE_HHLD_ID"));
		config.put("OVERRIDEREQUESTOR", (c) -> "CNX");
		config.put("DATEMODIFIED", (c) -> c.element().getString("CURRENT_TIME_STAMP"));

		return config;
	}

	public static Map<String, PGPEncryptFileIO.FieldFn<Row>> getCnxIdDeltaFile() {
		Map<String, PGPEncryptFileIO.FieldFn<Row>> config = new LinkedHashMap<>();
		config.put("DATASHARE_ID", (c) -> c.element().getString("DATASHARE_ID"));
		config.put("WAAS_CNX_ID", (c) -> c.element().getString("EFX_OVERRIDE_CNX_ID"));
		config.put("CNX_ID", (c) -> c.element().getString("EFX_CNX_ID"));
		config.put("WAS_HHLD_ID", (c) -> c.element().getString("EFX_OVERRIDE_HHLD_ID"));
		config.put("HHLD_ID", (c) -> c.element().getString("EFX_HHLD_ID"));
		config.put("WAS_ADDR_ID", (c) -> c.element().getString("EFX_ADDR_ID"));
		config.put("ADDR_ID", (c) -> c.element().getString("EFX_ADDR_ID"));

		return config;
	}

	public static Map<String, FieldFn<Row>> getConfigForExtractDivzcommsR00VztDsProdKrspF() {
		Map<String, PGPEncryptFileIO.FieldFn<Row>> config = new LinkedHashMap<>();
		config.put("UNIQUE_VZT_ID", (c) -> c.element().getString("UNIQUE_VZT_ID"));
		config.put("INDIVIDUAL_ID", (c) -> c.element().getString("INDIVIDUAL_ID"));
		config.put("HOUSEHOLD_ID", (c) -> c.element().getString("HOUSEHOLD_ID"));
		config.put("ADDRESS_ID", (c) -> c.element().getString("ADDRESS_ID"));
		config.put("EFX_ID", (c) -> null);
		config.put("HQ_ID", (c) -> null);
		config.put("DOMESTIC_ULTIMATE_ID", (c) -> null);
		config.put("GLOBAL_ULTIMATE_ID", (c) -> null);
		config.put("ERROR_CODE",
				(c) -> (ObjectUtils.transformationForEfxErrorCode(c.element().getString("ERROR_CODE"))));
		config.put("BEST_ADDR_FLAG", (c) -> c.element().getString("BEST_ADDR_FLAG"));
		config.put("CONFIDENCELEVEL", (c) -> c.element().getString("CONFIDENCELEVEL"));

		return config;
	}

	public static Map<String, FieldFn<Row>> getConfigForExtractDataShareSsaConInputFile() {
		Map<String, PGPEncryptFileIO.FieldFn<Row>> config = new LinkedHashMap<>();
		config.put("EFX_UNIQ_ID", (c) -> c.element().getDecimal("EFX_UNIQ_ID").toString());
		config.put("EFX_NM", (c) -> c.element().getString("EFX_NM"));
		return config;
	}

}
