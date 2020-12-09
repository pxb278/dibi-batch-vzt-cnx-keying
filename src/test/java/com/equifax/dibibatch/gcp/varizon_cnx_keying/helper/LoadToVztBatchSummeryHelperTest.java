package com.equifax.dibibatch.gcp.varizon_cnx_keying.helper;

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

public class LoadToVztBatchSummeryHelperTest {

	@Test
	public void readForLoadVztBatchSummeryRow() throws Exception {
		ResultSet resultSet = spy(ResultSet.class);

		BigDecimal LOAD_CNT = new BigDecimal(42);

		when(resultSet.getBigDecimal("LOAD_CNT")).thenReturn(LOAD_CNT);

		Row row = LoadToVztBatchSummeryHelper.readForLoadVztBatchSummeryRow(resultSet);

		Assert.assertNotNull(row);
		verify(resultSet).getBigDecimal(eq("LOAD_CNT"));

	}

	@Test
	public void insertIntoVztBatch() throws Exception {

		PreparedStatement preparedStatement = spy(PreparedStatement.class);
		ValueProvider<String> currentTimestamp = mock(ValueProvider.class);
		when(currentTimestamp.get()).thenReturn("07-07-2020");

		ValueProvider<Integer> batchId = mock(ValueProvider.class);
		when(batchId.get()).thenReturn(80);

		ValueProvider<Integer> ultimateBatchId = mock(ValueProvider.class);
		when(ultimateBatchId.get()).thenReturn(80);

		ValueProvider<String> vztCnxRespFile = mock(ValueProvider.class);
		when(vztCnxRespFile.get()).thenReturn("vztDSRespFileName");

		ValueProvider<Integer> vztCnxRespFileCount = mock(ValueProvider.class);
		when(vztCnxRespFileCount.get()).thenReturn(80);

		Row row = mock(Row.class);

		when(row.getDecimal(eq(1))).thenReturn(BigDecimal.valueOf(80));

		LoadToVztBatchSummeryHelper.insertIntoVztBatch(row, preparedStatement, currentTimestamp, batchId,
				vztCnxRespFile, vztCnxRespFileCount, ultimateBatchId);

		verify(preparedStatement).setBigDecimal(eq(1), eq((BigDecimal.valueOf(80))));
	}

	@Test
	public void insertIntoVztDataSummary() throws Exception {

		PreparedStatement preparedStatement = spy(PreparedStatement.class);
		ValueProvider<String> currentTimestamp = mock(ValueProvider.class);
		when(currentTimestamp.get()).thenReturn("07-07-2020");

		ValueProvider<Integer> batchId = mock(ValueProvider.class);
		when(batchId.get()).thenReturn(80);

		ValueProvider<Integer> ultimateBatchId = mock(ValueProvider.class);
		when(ultimateBatchId.get()).thenReturn(80);

		ValueProvider<String> vztCnxRespFile = mock(ValueProvider.class);
		when(vztCnxRespFile.get()).thenReturn("vztDSRespFileName");

		ValueProvider<Integer> vztCnxRespFileCount = mock(ValueProvider.class);
		when(vztCnxRespFileCount.get()).thenReturn(80);

		Row row = mock(Row.class);

		when(row.getDecimal(eq(1))).thenReturn(BigDecimal.valueOf(80));

		LoadToVztBatchSummeryHelper.insertIntoVztDataSummary(row, preparedStatement, currentTimestamp, batchId,
				vztCnxRespFile, vztCnxRespFileCount, ultimateBatchId);

		verify(preparedStatement).setBigDecimal(eq(1), eq((BigDecimal.valueOf(80))));
	}

	@Test
	public void updateVztDataSummary() throws Exception {

		PreparedStatement preparedStatement = spy(PreparedStatement.class);
		ValueProvider<String> currentTimestamp = mock(ValueProvider.class);
		when(currentTimestamp.get()).thenReturn("07-07-2020");

		ValueProvider<Integer> batchId = mock(ValueProvider.class);
		when(batchId.get()).thenReturn(80);

		ValueProvider<Integer> ultimateBatchId = mock(ValueProvider.class);
		when(ultimateBatchId.get()).thenReturn(80);

		ValueProvider<String> vztCnxRespFile = mock(ValueProvider.class);
		when(vztCnxRespFile.get()).thenReturn("vztDSRespFileName");

		ValueProvider<Integer> vztCnxRespFileCount = mock(ValueProvider.class);
		when(vztCnxRespFileCount.get()).thenReturn(80);

		Row row = mock(Row.class);

		when(row.getDecimal(eq(1))).thenReturn(BigDecimal.valueOf(80));

		LoadToVztBatchSummeryHelper.updateVztDataSummary(row, preparedStatement, currentTimestamp, batchId,
				vztCnxRespFile, vztCnxRespFileCount, ultimateBatchId);

		verify(preparedStatement).setBigDecimal(eq(1), eq((BigDecimal.valueOf(80))));
	}
}
