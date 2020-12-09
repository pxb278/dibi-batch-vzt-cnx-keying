package com.equifax.dibibatch.gcp.varizon_cnx_keying.sql;

public class ExtractDivzcommsR00VztDsProdKrspSql {

	public static final String SELECT_FROM_VW_VZT_DATASHARE = "SELECT DATASHARE_ID, EFX_CNX_ID, EFX_HHLD_ID, EFX_ADDR_ID, EFX_ERROR_CODE, EFX_BEST_KEY_SOURCE, EFX_CONF_CD FROM {{1}}.VW_VZT_DATASHARE_CNX_RESP WHERE EFX_CNX_MODIFY_BATCH_ID = ? AND KEYING_FLAG='T'";

}
