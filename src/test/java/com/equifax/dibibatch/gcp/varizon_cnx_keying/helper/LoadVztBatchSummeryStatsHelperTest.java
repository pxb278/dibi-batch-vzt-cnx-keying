package com.equifax.dibibatch.gcp.varizon_cnx_keying.helper;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.sql.PreparedStatement;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.helper.LoadVztBatchSummeryStatsHelper;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.CalendarUtil;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcCommons;

public class LoadVztBatchSummeryStatsHelperTest {
	
	@Test
	public void insertVztBatchTest() throws Exception {

		PreparedStatement preparedStatement = spy(PreparedStatement.class);

		ValueProvider<String> currentTimestamp = mock(ValueProvider.class);
		when(currentTimestamp.get()).thenReturn("07-07-2020");
		
		ValueProvider<String> srcFileName = mock(ValueProvider.class);
		when(srcFileName.get()).thenReturn("07-07-2020");
		
		ValueProvider<Integer> batchId = mock(ValueProvider.class);
		when(batchId.get()).thenReturn(80);
		
		ValueProvider<Integer> ultimateBatchId = mock(ValueProvider.class);
		when(ultimateBatchId.get()).thenReturn(9);
		
		ValueProvider<Integer> CNXExtractCount = mock(ValueProvider.class);
		when(CNXExtractCount.get()).thenReturn(8);
		
		ValueProvider<Integer> sourceVbbRecordCount = mock(ValueProvider.class);
		when(sourceVbbRecordCount.get()).thenReturn(6);
		
		ValueProvider<Integer> sourceVzbRecordCount = mock(ValueProvider.class);
		when(sourceVzbRecordCount.get()).thenReturn(5);
		
		ValueProvider<Integer> sourceVztRecordCount = mock(ValueProvider.class);
		when(sourceVztRecordCount.get()).thenReturn(8);
		
		ValueProvider<Integer> sourceVzwRecordCount = mock(ValueProvider.class);
		when(sourceVzwRecordCount.get()).thenReturn(9);
		
		ValueProvider<Integer> inputfileFinalCount = mock(ValueProvider.class);
		when(inputfileFinalCount.get()).thenReturn(9);
		
		ValueProvider<Integer> inputfileLiveCount = mock(ValueProvider.class);
		when(inputfileLiveCount.get()).thenReturn(9);
		
		ValueProvider<Integer> inputFileRecCount = mock(ValueProvider.class);
		when(inputFileRecCount.get()).thenReturn(9);
		
		ValueProvider<Integer> vztInsertCnt = mock(ValueProvider.class);
		when(vztInsertCnt.get()).thenReturn(9);
		
		ValueProvider<String> keyingFlagValueF = mock(ValueProvider.class);
		when(keyingFlagValueF.get()).thenReturn("F");
		
		ValueProvider<String> keyingFlagValueT = mock(ValueProvider.class);
		when(keyingFlagValueT.get()).thenReturn("T");
		
		ValueProvider<String> vztCnxReqFile = mock(ValueProvider.class);
		when(vztCnxReqFile.get()).thenReturn("vztCnxReqFile");
		
		ValueProvider<String> inputFileDate = mock(ValueProvider.class);
		when(inputFileDate.get()).thenReturn("07-07-2020");
		

		Row row = mock(Row.class);
		when(row.getDecimal(eq("LOAD_CNT"))).thenReturn(BigDecimal.valueOf(4));

		LoadVztBatchSummeryStatsHelper.insertVztBatch(row, preparedStatement, currentTimestamp, batchId, srcFileName, ultimateBatchId);
		verify(preparedStatement).setString(eq(4), eq("VZT DATASHARE REQUEST FILE LOAD"));
		verify(preparedStatement).setBigDecimal(eq(5), eq(BigDecimal.valueOf(4)));
		verify(preparedStatement).setBigDecimal(eq(6), eq(null));
		verify(preparedStatement).setBigDecimal(eq(9), eq(BigDecimal.valueOf(4)));
		verify(preparedStatement).setString(eq(10), eq("Processed DataShare Request file and Sent Cnx Key Request File"));	
	}

	@Test(expected = Exception.class)
	public void insertVztBatchTest_exception() throws Exception {

		PreparedStatement preparedStatement = spy(PreparedStatement.class);

		ValueProvider<String> currentTimestamp = mock(ValueProvider.class);
		when(currentTimestamp.get()).thenReturn("07-07-2020");

		
		ValueProvider<String> srcFileName = mock(ValueProvider.class);
		when(srcFileName.get()).thenReturn("07-07-2020");
		
		ValueProvider<Integer> batchId = mock(ValueProvider.class);
		when(batchId.get()).thenReturn(80);
		
		ValueProvider<Integer> ultimateBatchId = mock(ValueProvider.class);
		when(ultimateBatchId.get()).thenReturn(9);
		
		ValueProvider<Integer> CNXExtractCount = mock(ValueProvider.class);
		when(CNXExtractCount.get()).thenReturn(8);
		
		ValueProvider<Integer> sourceVbbRecordCount = mock(ValueProvider.class);
		when(sourceVbbRecordCount.get()).thenReturn(6);
		
		ValueProvider<Integer> sourceVzbRecordCount = mock(ValueProvider.class);
		when(sourceVzbRecordCount.get()).thenReturn(5);
		
		ValueProvider<Integer> sourceVztRecordCount = mock(ValueProvider.class);
		when(sourceVztRecordCount.get()).thenReturn(8);
		
		ValueProvider<Integer> sourceVzwRecordCount = mock(ValueProvider.class);
		when(sourceVzwRecordCount.get()).thenReturn(9);
		
		ValueProvider<Integer> inputfileFinalCount = mock(ValueProvider.class);
		when(inputfileFinalCount.get()).thenReturn(9);
		
		ValueProvider<Integer> inputfileLiveCount = mock(ValueProvider.class);
		when(inputfileLiveCount.get()).thenReturn(9);
		
		ValueProvider<Integer> inputFileRecCount = mock(ValueProvider.class);
		when(inputFileRecCount.get()).thenReturn(9);
		
		ValueProvider<Integer> vztInsertCnt = mock(ValueProvider.class);
		when(vztInsertCnt.get()).thenReturn(9);
		
		ValueProvider<String> keyingFlagValueF = mock(ValueProvider.class);
		when(keyingFlagValueF.get()).thenReturn("F");
		
		ValueProvider<String> keyingFlagValueT = mock(ValueProvider.class);
		when(keyingFlagValueT.get()).thenReturn("T");
		
		ValueProvider<String> vztCnxReqFile = mock(ValueProvider.class);
		when(vztCnxReqFile.get()).thenReturn("vztCnxReqFile");
		
		ValueProvider<String> inputFileDate = mock(ValueProvider.class);
		when(inputFileDate.get()).thenReturn("07-07-2020");
		
		Row row = mock(Row.class);
		when(row.getString(eq("BATCH_ID"))).thenThrow(new Exception());

		LoadVztBatchSummeryStatsHelper.insertVztBatch(row, preparedStatement, currentTimestamp, batchId, srcFileName, ultimateBatchId);
	}

	@Test
	public void insertVztBatchSummaryTest() throws Exception {


		PreparedStatement preparedStatement = spy(PreparedStatement.class);

		ValueProvider<String> currentTimestamp = mock(ValueProvider.class);
		when(currentTimestamp.get()).thenReturn("07-07-2020");

		
		ValueProvider<String> srcFileName = mock(ValueProvider.class);
		when(srcFileName.get()).thenReturn("srcFileName");
		
		ValueProvider<Integer> batchId = mock(ValueProvider.class);
		when(batchId.get()).thenReturn(80);
		
		ValueProvider<Integer> ultimateBatchId = mock(ValueProvider.class);
		when(ultimateBatchId.get()).thenReturn(9);
		
		ValueProvider<Integer> CNXExtractCount = mock(ValueProvider.class);
		when(CNXExtractCount.get()).thenReturn(8);
		
		ValueProvider<Integer> sourceVbbRecordCount = mock(ValueProvider.class);
		when(sourceVbbRecordCount.get()).thenReturn(6);
		
		ValueProvider<Integer> sourceVzbRecordCount = mock(ValueProvider.class);
		when(sourceVzbRecordCount.get()).thenReturn(5);
		
		ValueProvider<Integer> sourceVztRecordCount = mock(ValueProvider.class);
		when(sourceVztRecordCount.get()).thenReturn(8);
		
		ValueProvider<Integer> sourceVzwRecordCount = mock(ValueProvider.class);
		when(sourceVzwRecordCount.get()).thenReturn(9);
		
		ValueProvider<Integer> inputfileFinalCount = mock(ValueProvider.class);
		when(inputfileFinalCount.get()).thenReturn(9);
		
		ValueProvider<Integer> inputfileLiveCount = mock(ValueProvider.class);
		when(inputfileLiveCount.get()).thenReturn(9);
		
		ValueProvider<Integer> inputFileRecCount = mock(ValueProvider.class);
		when(inputFileRecCount.get()).thenReturn(9);
		
		ValueProvider<Integer> vztInsertCnt = mock(ValueProvider.class);
		when(vztInsertCnt.get()).thenReturn(9);
		
		ValueProvider<Integer> keyingFlagValueF = mock(ValueProvider.class);
		when(keyingFlagValueF.get()).thenReturn(8);
		
		ValueProvider<Integer> keyingFlagValueT = mock(ValueProvider.class);
		when(keyingFlagValueT.get()).thenReturn(9);
		
		ValueProvider<String> vztCnxReqFile = mock(ValueProvider.class);
		when(vztCnxReqFile.get()).thenReturn("vztCnxReqFile");
		
		ValueProvider<String> inputFileDate = mock(ValueProvider.class);
		when(inputFileDate.get()).thenReturn("07-07-2020");
		
		Row row = mock(Row.class);
		
		when(row.getDecimal(eq("LOAD_CNT"))).thenReturn(BigDecimal.valueOf(4));

		LoadVztBatchSummeryStatsHelper.insertVztBatchSummary
		(row, preparedStatement, currentTimestamp, batchId, ultimateBatchId, srcFileName, inputFileRecCount, keyingFlagValueF, keyingFlagValueT
				, CNXExtractCount, vztCnxReqFile);
		
		verify(preparedStatement).setBigDecimal(eq(6), eq(BigDecimal.valueOf(4)));
		
	}

	@Test(expected = Exception.class)
	public void insertVztBatchSummaryTest_exception() throws Exception {

		PreparedStatement preparedStatement = spy(PreparedStatement.class);

		ValueProvider<String> currentTimestamp = mock(ValueProvider.class);
		when(currentTimestamp.get()).thenReturn("07-07-2020");
		
		ValueProvider<String> srcFileName = mock(ValueProvider.class);
		when(srcFileName.get()).thenReturn("srcFileName");
		
		ValueProvider<Integer> batchId = mock(ValueProvider.class);
		when(batchId.get()).thenReturn(80);
		
		ValueProvider<Integer> ultimateBatchId = mock(ValueProvider.class);
		when(ultimateBatchId.get()).thenReturn(9);
		
		ValueProvider<Integer> CNXExtractCount = mock(ValueProvider.class);
		when(CNXExtractCount.get()).thenReturn(8);
		
		ValueProvider<Integer> sourceVbbRecordCount = mock(ValueProvider.class);
		when(sourceVbbRecordCount.get()).thenReturn(6);
		
		ValueProvider<Integer> sourceVzbRecordCount = mock(ValueProvider.class);
		when(sourceVzbRecordCount.get()).thenReturn(5);
		
		ValueProvider<Integer> sourceVztRecordCount = mock(ValueProvider.class);
		when(sourceVztRecordCount.get()).thenReturn(8);
		
		ValueProvider<Integer> sourceVzwRecordCount = mock(ValueProvider.class);
		when(sourceVzwRecordCount.get()).thenReturn(9);
		
		ValueProvider<Integer> inputfileFinalCount = mock(ValueProvider.class);
		when(inputfileFinalCount.get()).thenReturn(9);
		
		ValueProvider<Integer> inputfileLiveCount = mock(ValueProvider.class);
		when(inputfileLiveCount.get()).thenReturn(9);
		
		ValueProvider<Integer> inputFileRecCount = mock(ValueProvider.class);
		when(inputFileRecCount.get()).thenReturn(9);
		
		ValueProvider<Integer> vztInsertCnt = mock(ValueProvider.class);
		when(vztInsertCnt.get()).thenReturn(9);
		
		ValueProvider<Integer> keyingFlagValueF = mock(ValueProvider.class);
		when(keyingFlagValueF.get()).thenReturn(8);
		
		ValueProvider<Integer> keyingFlagValueT = mock(ValueProvider.class);
		when(keyingFlagValueT.get()).thenReturn(9);
		
		ValueProvider<String> vztCnxReqFile = mock(ValueProvider.class);
		when(vztCnxReqFile.get()).thenReturn("vztCnxReqFile");
		
		ValueProvider<String> inputFileDate = mock(ValueProvider.class);
		when(inputFileDate.get()).thenReturn("07-07-2020");

		Row row = mock(Row.class);
		when(row.getString(eq("ULTIMATE_BATCH_ID"))).thenThrow(new Exception());

		LoadVztBatchSummeryStatsHelper.insertVztBatchSummary(row, preparedStatement, currentTimestamp, batchId, ultimateBatchId, srcFileName, inputFileRecCount, keyingFlagValueF, keyingFlagValueT, CNXExtractCount, vztCnxReqFile);
	}
	
	
	@Test
	public void insertVztBatchStatTest() throws Exception {


		PreparedStatement preparedStatement = spy(PreparedStatement.class);

		ValueProvider<String> currentTimestamp = mock(ValueProvider.class);
		when(currentTimestamp.get()).thenReturn("07-07-2020");

		
		ValueProvider<String> srcFileName = mock(ValueProvider.class);
		when(srcFileName.get()).thenReturn("srcFileName");
		
		ValueProvider<Integer> batchId = mock(ValueProvider.class);
		when(batchId.get()).thenReturn(80);
		
		ValueProvider<Integer> ultimateBatchId = mock(ValueProvider.class);
		when(ultimateBatchId.get()).thenReturn(9);
		
		ValueProvider<Integer> CNXExtractCount = mock(ValueProvider.class);
		when(CNXExtractCount.get()).thenReturn(8);
		
		ValueProvider<Integer> sourceVbbRecordCount = mock(ValueProvider.class);
		when(sourceVbbRecordCount.get()).thenReturn(6);
		
		ValueProvider<Integer> sourceVzbRecordCount = mock(ValueProvider.class);
		when(sourceVzbRecordCount.get()).thenReturn(5);
		
		ValueProvider<Integer> sourceVztRecordCount = mock(ValueProvider.class);
		when(sourceVztRecordCount.get()).thenReturn(8);
		
		ValueProvider<Integer> sourceVzwRecordCount = mock(ValueProvider.class);
		when(sourceVzwRecordCount.get()).thenReturn(9);
		
		ValueProvider<Integer> sourceVbwRecordCount = mock(ValueProvider.class);
		when(sourceVbwRecordCount.get()).thenReturn(9);
		
		ValueProvider<Integer> inputfileFinalCount = mock(ValueProvider.class);
		when(inputfileFinalCount.get()).thenReturn(9);
		
		ValueProvider<Integer> inputfileLiveCount = mock(ValueProvider.class);
		when(inputfileLiveCount.get()).thenReturn(9);
		
		ValueProvider<Integer> inputFileRecCount = mock(ValueProvider.class);
		when(inputFileRecCount.get()).thenReturn(9);
		
		ValueProvider<Integer> vztInsertCnt = mock(ValueProvider.class);
		when(vztInsertCnt.get()).thenReturn(9);
		
		ValueProvider<Integer> keyingFlagValueF = mock(ValueProvider.class);
		when(keyingFlagValueF.get()).thenReturn(8);
		
		ValueProvider<Integer> keyingFlagValueT = mock(ValueProvider.class);
		when(keyingFlagValueT.get()).thenReturn(9);
		
		ValueProvider<String> vztCnxReqFile = mock(ValueProvider.class);
		when(vztCnxReqFile.get()).thenReturn("vztCnxReqFile");
		
		ValueProvider<String> inputFileDate = mock(ValueProvider.class);
		when(inputFileDate.get()).thenReturn("07-07-2020");
		
		Row row = mock(Row.class);
		
		when(row.getDecimal(eq("LOAD_CNT"))).thenReturn(BigDecimal.valueOf(4));

		LoadVztBatchSummeryStatsHelper.insertVztBatchStats(row, preparedStatement, currentTimestamp, batchId, ultimateBatchId, srcFileName, inputFileRecCount, vztInsertCnt, sourceVbbRecordCount, sourceVzbRecordCount, sourceVztRecordCount, inputfileLiveCount, inputfileFinalCount, sourceVbwRecordCount);
		
		verify(preparedStatement).setBigDecimal(eq(5), eq(BigDecimal.valueOf(4)));
		verify(preparedStatement).setBigDecimal(eq(15), eq(null));
		verify(preparedStatement).setBigDecimal(eq(16), eq(null));

		
	}

	@Test(expected = Exception.class)
	public void insertVztBatchStatsTest_exception() throws Exception {

		PreparedStatement preparedStatement = spy(PreparedStatement.class);

		ValueProvider<String> currentTimestamp = mock(ValueProvider.class);
		when(currentTimestamp.get()).thenReturn("07-07-2020");
		
		ValueProvider<String> srcFileName = mock(ValueProvider.class);
		when(srcFileName.get()).thenReturn("srcFileName");
		
		ValueProvider<Integer> batchId = mock(ValueProvider.class);
		when(batchId.get()).thenReturn(80);
		
		ValueProvider<Integer> ultimateBatchId = mock(ValueProvider.class);
		when(ultimateBatchId.get()).thenReturn(9);
		
		ValueProvider<Integer> CNXExtractCount = mock(ValueProvider.class);
		when(CNXExtractCount.get()).thenReturn(8);
		
		ValueProvider<Integer> sourceVbbRecordCount = mock(ValueProvider.class);
		when(sourceVbbRecordCount.get()).thenReturn(6);
		
		ValueProvider<Integer> sourceVzbRecordCount = mock(ValueProvider.class);
		when(sourceVzbRecordCount.get()).thenReturn(5);
		
		ValueProvider<Integer> sourceVztRecordCount = mock(ValueProvider.class);
		when(sourceVztRecordCount.get()).thenReturn(8);
		
		ValueProvider<Integer> sourceVzwRecordCount = mock(ValueProvider.class);
		when(sourceVzwRecordCount.get()).thenReturn(9);
		
		
		ValueProvider<Integer> sourceVbwRecordCount = mock(ValueProvider.class);
		when(sourceVbwRecordCount.get()).thenReturn(9);
		
		
		ValueProvider<Integer> inputfileFinalCount = mock(ValueProvider.class);
		when(inputfileFinalCount.get()).thenReturn(9);
		
		ValueProvider<Integer> inputfileLiveCount = mock(ValueProvider.class);
		when(inputfileLiveCount.get()).thenReturn(9);
		
		ValueProvider<Integer> inputFileRecCount = mock(ValueProvider.class);
		when(inputFileRecCount.get()).thenReturn(9);
		
		ValueProvider<Integer> vztInsertCnt = mock(ValueProvider.class);
		when(vztInsertCnt.get()).thenReturn(9);
		
		ValueProvider<Integer> keyingFlagValueF = mock(ValueProvider.class);
		when(keyingFlagValueF.get()).thenReturn(8);
		
		ValueProvider<Integer> keyingFlagValueT = mock(ValueProvider.class);
		when(keyingFlagValueT.get()).thenReturn(9);
		
		ValueProvider<String> vztCnxReqFile = mock(ValueProvider.class);
		when(vztCnxReqFile.get()).thenReturn("vztCnxReqFile");
		
		ValueProvider<String> inputFileDate = mock(ValueProvider.class);
		when(inputFileDate.get()).thenReturn("07-07-2020");

		Row row = mock(Row.class);
		when(row.getString(eq("ULTIMATE_BATCH_ID"))).thenThrow(new Exception());

		LoadVztBatchSummeryStatsHelper.insertVztBatchStats(row, preparedStatement, currentTimestamp, batchId, ultimateBatchId, srcFileName, inputFileRecCount, vztInsertCnt, sourceVbbRecordCount, sourceVzbRecordCount, sourceVztRecordCount, inputfileLiveCount, inputfileFinalCount, sourceVbwRecordCount);;
	}



}
