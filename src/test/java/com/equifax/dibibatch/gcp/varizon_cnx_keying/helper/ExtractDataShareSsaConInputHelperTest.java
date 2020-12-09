package com.equifax.dibibatch.gcp.varizon_cnx_keying.helper;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.sql.ResultSet;

import org.apache.beam.sdk.values.Row;
import org.junit.Assert;
import org.junit.Test;

public class ExtractDataShareSsaConInputHelperTest {

	@Test
	public void readExtractDataShareSsaConInputRow() throws Exception {
		ResultSet resultSet = spy(ResultSet.class);

		BigDecimal VZ_DATASHARE_CNX_REPO_ID = new BigDecimal(42);

		when(resultSet.getString(anyString())).thenReturn("1234");
		when(resultSet.getBigDecimal("VZ_DATASHARE_CNX_REPO_ID")).thenReturn(VZ_DATASHARE_CNX_REPO_ID);

		Row row = ExtractDataShareSsaConInputHelper.readExtractDataShareSsaConInputRow(resultSet);

		Assert.assertNotNull(row);
		verify(resultSet).getBigDecimal(eq("VZ_DATASHARE_CNX_REPO_ID"));
		verify(resultSet).getString(eq("FIRST_NAME"));

	}
}