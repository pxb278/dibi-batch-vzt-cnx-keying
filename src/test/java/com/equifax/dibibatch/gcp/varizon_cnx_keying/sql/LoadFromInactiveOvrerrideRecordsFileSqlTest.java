package com.equifax.dibibatch.gcp.varizon_cnx_keying.sql;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class LoadFromInactiveOvrerrideRecordsFileSqlTest {
	
	@Test
    public void testSqlQueryVztDataShareOne()
    {
		
		final String UPDATE_VZT_DATASHARE_TABLE_TEST = "UPDATE  {{1}}.VZT_DATASHARE SET EFX_OVERRIDE_STATUS = ? WHERE (DATASHARE_ID = ?)";

		String dataOne= LoadFromInactiveOvrerrideRecordsFileSql.UPDATE_VZT_DATASHARE_TABLE;

        assertTrue((dataOne.toString().compareToIgnoreCase(UPDATE_VZT_DATASHARE_TABLE_TEST) == 0));

    }
	
	@Test
    public void testSqlQueryVztDataShareTwo()
    {
		
		final String UPDATE_CONNEXUSKEYOVERRIDE_TABLE_TEST = "UPDATE {{1}}.CONNEXUSKEYOVERRIDE SET OVERRIDEREQUESTOR = ?, STATUS = ?, DATEMODIFIED = ? WHERE (DATASHAREID = ?)";

		String dataTwo= LoadFromInactiveOvrerrideRecordsFileSql.UPDATE_CONNEXUSKEYOVERRIDE_TABLE;

        assertTrue((dataTwo.toString().compareToIgnoreCase(UPDATE_CONNEXUSKEYOVERRIDE_TABLE_TEST) == 0));

    }

}
