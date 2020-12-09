package com.equifax.dibibatch.gcp.varizon_cnx_keying.sql;

public class ExtractInactivatedOverrideRecordsSql {

	public static final String SELECT_FROM_VW_VZT_DATASHARE = "SELECT DATASHARE_ID, OLD_BEST_CNX_ID, EFX_CNX_ID, EFX_HHLD_ID, EFX_ADDR_ID, EFX_OVERRIDE_CNX_ID, EFX_OVERRIDE_HHLD_ID FROM {{1}}.VW_VZT_DATASHARE\r\n"
			+ "WHERE EFX_CNX_MODIFY_BATCH_ID = ? AND EFX_OVERRIDE_STATUS = 'Active' AND COALESCE(EFX_CNX_ID, 'X') <> COALESCE(OLD_BEST_CNX_ID, 'Y')";

}
