package com.equifax.dibibatch.gcp.varizon_cnx_keying.helper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.values.Row;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

import com.equifax.usis.dibi.batch.gcp.dataflow.common.io.PGPEncryptFileIO.FieldFn;

@RunWith(JUnit4.class)
public class MonthlyCnxBatchSalesTrackHelperTest {
	@Test
	public void getMonthyCnxBatchSalesConfigTest() {
		@SuppressWarnings("rawtypes")
		ProcessContext c = mock(ProcessContext.class);

		Row row = mock(Row.class);
		
		when(row.getString(anyString())).thenReturn("test1");
		when(row.getString(anyString())).thenReturn("test2");
		when(row.getString(anyString())).thenReturn("test3");
		when(row.getString(anyString())).thenReturn("test4");

		when(c.element()).thenReturn(row);

		Map<String, FieldFn<Row>> config = MonthlyCnxBatchSalesTrackHelper.getMonthyCnxBatchSalesConfig();

		for (Map.Entry<String, FieldFn<Row>> entry : config.entrySet()) {
			FieldFn<Row> fcn = entry.getValue();
			@SuppressWarnings("unchecked")
			String result = (String) fcn.apply(c);
			assertThat(result, Matchers.any(String.class));
		}
	}
	
	@Test
	public void rowBuilderMonthlyCnxBatchSalesTest() throws Exception {
		ResultSet resultSet = Mockito.mock(ResultSet.class);
		MonthlyCnxBatchSalesTrackHelper.rowBuilderMonthlyCnxBatchSales(resultSet);
	}
}
