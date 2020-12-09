package com.equifax.dibibatch.gcp.varizon_cnx_keying.sql;

public class ExtractDataShareSsaConInputSql {

	public static final String SELECT_VZ_DATASHARE_CNX_REPO_TABLE = "SELECT VZ_DATASHARE_CNX_REPO_ID,FIRST_NAME, LAST_NAME  FROM {{1}}.VZ_DATASHARE_CNX_REPO WHERE EFX_MODIFY_BATCH_ID=?";

}
