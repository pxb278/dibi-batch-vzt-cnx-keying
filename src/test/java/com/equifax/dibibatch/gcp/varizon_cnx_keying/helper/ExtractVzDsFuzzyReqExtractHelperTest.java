package com.equifax.dibibatch.gcp.varizon_cnx_keying.helper;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;

import org.apache.beam.sdk.values.Row;
import org.junit.Assert;
import org.junit.Test;

public class ExtractVzDsFuzzyReqExtractHelperTest {

	@Test
	public void readExtractVzDsFuzzyReqExtractRow() throws Exception {
		ResultSet resultSet = spy(ResultSet.class);

		when(resultSet.getString(anyString())).thenReturn("1234");

		Row row = ExtractVzDsFuzzyReqExtractHelper.readExtractVzDsFuzzyReqExtractRow(resultSet);

		Assert.assertNotNull(row);
		verify(resultSet).getString(eq("SURROGATE_KEY"));

	}
}
