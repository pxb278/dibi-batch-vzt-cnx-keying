package com.equifax.dibibatch.gcp.varizon_cnx_keying.helper;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.Row;
import org.junit.Assert;
import org.junit.Test;

public class LoadTablesAsPerRowGeneratorHelperTest {

	@Test
	public void insertVztBatch() throws Exception {

		PreparedStatement preparedStatement = spy(PreparedStatement.class);
		ValueProvider<String> currentTimestamp = mock(ValueProvider.class);
		when(currentTimestamp.get()).thenReturn("07-07-2020");

		ValueProvider<Integer> batchId = mock(ValueProvider.class);
		when(batchId.get()).thenReturn(80);

		ValueProvider<Integer> ultimateBatchId = mock(ValueProvider.class);
		when(ultimateBatchId.get()).thenReturn(80);

		ValueProvider<String> vztDSRespFileName = mock(ValueProvider.class);
		when(vztDSRespFileName.get()).thenReturn("vztDSRespFileName");

		ValueProvider<Integer> vztDSRespFileCount = mock(ValueProvider.class);
		when(vztDSRespFileCount.get()).thenReturn(80);

		Row row = mock(Row.class);

		when(row.getDecimal(eq(1))).thenReturn(BigDecimal.valueOf(80));

		LoadTablesAsPerRowGeneratorHelper.insertVztBatch(row, preparedStatement, currentTimestamp, ultimateBatchId,
				batchId, vztDSRespFileCount, vztDSRespFileName);

		verify(preparedStatement).setBigDecimal(eq(1), eq((BigDecimal.valueOf(80))));
	}

	@Test
	public void insertVztBatchSummary() throws Exception {

		PreparedStatement preparedStatement = spy(PreparedStatement.class);
		ValueProvider<String> currentTimestamp = mock(ValueProvider.class);
		when(currentTimestamp.get()).thenReturn("07-07-2020");

		ValueProvider<Integer> batchId = mock(ValueProvider.class);
		when(batchId.get()).thenReturn(80);

		ValueProvider<Integer> ultimateBatchId = mock(ValueProvider.class);
		when(ultimateBatchId.get()).thenReturn(80);

		ValueProvider<String> vztDSRespFileName = mock(ValueProvider.class);
		when(vztDSRespFileName.get()).thenReturn("vztDSRespFileName");

		ValueProvider<Integer> vztDSRespFileCount = mock(ValueProvider.class);
		when(vztDSRespFileCount.get()).thenReturn(80);

		Row row = mock(Row.class);

		when(row.getDecimal(eq(1))).thenReturn(BigDecimal.valueOf(80));

		LoadTablesAsPerRowGeneratorHelper.insertVztBatchSummary(row, preparedStatement, currentTimestamp,
				ultimateBatchId, batchId, vztDSRespFileName, vztDSRespFileCount);

		verify(preparedStatement).setBigDecimal(eq(1), eq((BigDecimal.valueOf(80))));
	}

	@Test
	public void updateVztBatchSummary() throws Exception {

		PreparedStatement preparedStatement = spy(PreparedStatement.class);
		ValueProvider<String> currentTimestamp = mock(ValueProvider.class);
		when(currentTimestamp.get()).thenReturn("07-07-2020");

		ValueProvider<Integer> batchId = mock(ValueProvider.class);
		when(batchId.get()).thenReturn(80);

		ValueProvider<Integer> ultimateBatchId = mock(ValueProvider.class);
		when(ultimateBatchId.get()).thenReturn(80);

		ValueProvider<String> vztDSRespFileName = mock(ValueProvider.class);
		when(vztDSRespFileName.get()).thenReturn("vztDSRespFileName");

		ValueProvider<Integer> vztDSRespFileCount = mock(ValueProvider.class);
		when(vztDSRespFileCount.get()).thenReturn(80);

		Row row = mock(Row.class);

		when(row.getDecimal(eq(1))).thenReturn(BigDecimal.valueOf(80));

		LoadTablesAsPerRowGeneratorHelper.updateVztBatchSummary(row, preparedStatement, currentTimestamp,
				ultimateBatchId, batchId, vztDSRespFileName, vztDSRespFileCount);

		verify(preparedStatement).setBigDecimal(eq(1), eq((BigDecimal.valueOf(80))));
	}

	@Test
	public void updateVztStat() throws Exception {

		PreparedStatement preparedStatement = spy(PreparedStatement.class);

		ValueProvider<Integer> vztHitCount = mock(ValueProvider.class);
		when(vztHitCount.get()).thenReturn(80);

		ValueProvider<Integer> ultimateBatchId = mock(ValueProvider.class);
		when(ultimateBatchId.get()).thenReturn(80);

		ValueProvider<Integer> vztNoHitCount = mock(ValueProvider.class);
		when(vztNoHitCount.get()).thenReturn(80);

		Row row = mock(Row.class);

		when(row.getDecimal(eq(1))).thenReturn(BigDecimal.valueOf(80));

		LoadTablesAsPerRowGeneratorHelper.updateVztStat(row, preparedStatement, ultimateBatchId, vztHitCount,
				vztNoHitCount);

		verify(preparedStatement).setBigDecimal(eq(1), eq((BigDecimal.valueOf(80))));
	}
	
	@Test
	public void readForLoadVztBatchSummeryRow() throws Exception {
		ResultSet resultSet = spy(ResultSet.class);

		BigDecimal ULTIMATE_BATCH_ID = new BigDecimal(42);

		when(resultSet.getString("VZT_OUTPUT_FILE_NAME")).thenReturn("VZT_OUTPUT_FILE_NAME");
		when(resultSet.getBigDecimal("ULTIMATE_BATCH_ID")).thenReturn(ULTIMATE_BATCH_ID);

		Row row = LoadTablesAsPerRowGeneratorHelper.readForLoadVztBatchSummeryRow(resultSet);

		Assert.assertNotNull(row);
		verify(resultSet).getBigDecimal(eq("ULTIMATE_BATCH_ID"));
		verify(resultSet).getString(eq("VZT_OUTPUT_FILE_NAME"));

	}
}
