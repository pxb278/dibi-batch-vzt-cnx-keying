package com.equifax.dibibatch.gcp.varizon_cnx_keying.sql;

public class LoadFromInactiveOvrerrideRecordsFileSql {

	public static final String UPDATE_VZT_DATASHARE_TABLE = "UPDATE  {{1}}.VZT_DATASHARE SET EFX_OVERRIDE_STATUS = ? WHERE (DATASHARE_ID = ?)";
	public static final String UPDATE_CONNEXUSKEYOVERRIDE_TABLE = "UPDATE {{1}}.CONNEXUSKEYOVERRIDE SET OVERRIDEREQUESTOR = ?, STATUS = ?, DATEMODIFIED = ? WHERE (DATASHAREID = ?)";

}
