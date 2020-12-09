package com.equifax.dibibatch.gcp.varizon_cnx_keying.helper;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.helper.ExtractStDsFuzzyRespToLoadVztDataShareHelper;

public class ExtractStDsFuzzyRespToLoadVztDataShareHelperTest {

	@Test
	public void updateVztDataShareRowWithKeyingFlagFTest() throws Exception {

		PreparedStatement preparedStatement = spy(PreparedStatement.class);

		ValueProvider<String> currentTimestamp = mock(ValueProvider.class);
		when(currentTimestamp.get()).thenReturn("07-07-2020");

		ValueProvider<Integer> batchId = mock(ValueProvider.class);
		when(batchId.get()).thenReturn(80);

		ValueProvider<String> processName = mock(ValueProvider.class);
		when(processName.get()).thenReturn("Verizon CNX Keying");

		Row row = mock(Row.class);
		
		when(row.getString(eq("B_CONF_CD"))).thenReturn("123");
		when(row.getString(eq("S_CONF_CD"))).thenReturn("123");

		if (null != row.getString("B_CONF_CD") || null != row.getString("S_CONF_CD")) {
		when(row.getString(eq("B_MATCHED_DS_ID"))).thenReturn("123");
		}
		ExtractStDsFuzzyRespToLoadVztDataShareHelper.updateVztDataShareRowFromStDsOut(row, preparedStatement,
				currentTimestamp, batchId, processName);
		verify(preparedStatement).setString(eq(1), eq("123"));

	}

}