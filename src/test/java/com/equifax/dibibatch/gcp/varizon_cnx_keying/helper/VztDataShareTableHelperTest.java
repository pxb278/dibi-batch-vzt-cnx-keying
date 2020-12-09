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

import com.equifax.dibibatch.gcp.varizon_cnx_keying.helper.VztDataShareTableHelper;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcCommons;

public class VztDataShareTableHelperTest {

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

		when(row.getString(eq("LINE_OF_BUSINESS"))).thenReturn("123");
		when(row.getString(eq("BTN"))).thenReturn("123");
		when(row.getString(eq("BILL_STREET_NO"))).thenReturn("123");
		when(row.getString(eq("BILL_STREET_NAME"))).thenReturn("123");
		when(row.getString(eq("BILL_CITY"))).thenReturn("123");
		when(row.getString(eq("BILL_STATE"))).thenReturn("123");
		when(row.getString(eq("BILL_ZIP"))).thenReturn("123");
		when(row.getString(eq("SVC_STREET_NO"))).thenReturn("123");
		when(row.getString(eq("SVC_STREET_NAME"))).thenReturn("123");
		when(row.getString(eq("SVC_CITY"))).thenReturn("123");
		when(row.getString(eq("SVC_STATE"))).thenReturn("123");
		when(row.getString(eq("SVC_ZIP"))).thenReturn("123");
		when(row.getString(eq("FIRST_NAME"))).thenReturn("123");
		when(row.getString(eq("MIDDLE_NAME"))).thenReturn("123");
		when(row.getString(eq("LAST_NAME"))).thenReturn("123");
		when(row.getString(eq("BUSINESS_NAME"))).thenReturn("123");
		when(row.getString(eq("SSN_TAXID"))).thenReturn("123");
		when(row.getString(eq("ACCOUNT_NO"))).thenReturn("123");
		when(row.getString(eq("LIVE_FINAL_INDICATOR"))).thenReturn("123");
		when(row.getString(eq("SOURCE_BUSINESS"))).thenReturn("123");
		when(row.getString(eq("FIBER_INDICATOR"))).thenReturn("123");
		when(row.getString(eq("KEYING_FLAG"))).thenReturn("F");
		when(row.getString(eq("EFX_SVC_CNX_ID"))).thenReturn("123");
		when(row.getString(eq("EFX_SVC_HHLD_ID"))).thenReturn("123");
		when(row.getString(eq("EFX_SVC_ADDR_ID"))).thenReturn("123");
		when(row.getString(eq("BTN_HMAC"))).thenReturn("123");
		when(row.getString(eq("BILL_STREET_NO_HMAC"))).thenReturn("123");
		when(row.getString(eq("BILL_STREET_NAME_HMAC"))).thenReturn("123");
		when(row.getString(eq("BILL_CITY_HMAC"))).thenReturn("123");
		when(row.getString(eq("BILL_STATE_HMAC"))).thenReturn("123");
		when(row.getString(eq("BILL_ZIP_HMAC"))).thenReturn("123");
		when(row.getString(eq("SVC_STREET_NO_HMAC"))).thenReturn("123");
		when(row.getString(eq("SVC_STREET_NAME_HMAC"))).thenReturn("123");
		when(row.getString(eq("SVC_CITY_HMAC"))).thenReturn("123");
		when(row.getString(eq("SVC_STATE_HMAC"))).thenReturn("123");
		when(row.getString(eq("SVC_ZIP_HMAC"))).thenReturn("123");
		when(row.getString(eq("FIRST_NAME_HMAC"))).thenReturn("123");
		when(row.getString(eq("MIDDLE_NAME_HMAC"))).thenReturn("123");
		when(row.getString(eq("LAST_NAME_HMAC"))).thenReturn("123");
		when(row.getString(eq("BUSINESS_NAME_HMAC"))).thenReturn("123");
		when(row.getString(eq("SSN_TAXID_HMAC"))).thenReturn("123");
		when(row.getString(eq("ACCOUNT_NO_HMAC"))).thenReturn("123");
		when(row.getString(eq("DATASHARE_ID"))).thenReturn("123");

		VztDataShareTableHelper.updateVztDataShareRowWithKeyingFlagF(row, preparedStatement, currentTimestamp, batchId,
				processName);

		verify(preparedStatement).setString(eq(1), eq("123"));
		verify(preparedStatement).setString(eq(2), eq("123"));
		verify(preparedStatement).setString(eq(3), eq("123"));
		verify(preparedStatement).setString(eq(4), eq("123"));
		verify(preparedStatement).setString(eq(5), eq("123"));
		verify(preparedStatement).setString(eq(6), eq("123"));
		verify(preparedStatement).setString(eq(7), eq("123"));
		verify(preparedStatement).setString(eq(8), eq("123"));
		verify(preparedStatement).setString(eq(9), eq("123"));
		verify(preparedStatement).setString(eq(10), eq("123"));
		verify(preparedStatement).setString(eq(11), eq("123"));
		verify(preparedStatement).setString(eq(12), eq("123"));
		verify(preparedStatement).setString(eq(13), eq("123"));
		verify(preparedStatement).setString(eq(14), eq("123"));
		verify(preparedStatement).setString(eq(15), eq("123"));
		verify(preparedStatement).setString(eq(16), eq("123"));
		verify(preparedStatement).setString(eq(17), eq("123"));
		verify(preparedStatement).setString(eq(18), eq("123"));
		verify(preparedStatement).setString(eq(19), eq("123"));
		verify(preparedStatement).setString(eq(20), eq("123"));
		verify(preparedStatement).setString(eq(21), eq("123"));
		verify(preparedStatement).setString(eq(22), eq("F"));
		verify(preparedStatement).setString(eq(23), eq("123"));
		verify(preparedStatement).setString(eq(24), eq("123"));
		verify(preparedStatement).setString(eq(25), eq("123"));
		verify(preparedStatement).setString(eq(27), eq("S"));

		verify(preparedStatement).setString(eq(31), eq("123"));
		verify(preparedStatement).setString(eq(32), eq("123"));
		verify(preparedStatement).setString(eq(33), eq("123"));
		verify(preparedStatement).setString(eq(34), eq("123"));
		verify(preparedStatement).setString(eq(35), eq("123"));
		verify(preparedStatement).setString(eq(36), eq("123"));
		verify(preparedStatement).setString(eq(37), eq("123"));
		verify(preparedStatement).setString(eq(38), eq("123"));
		verify(preparedStatement).setString(eq(39), eq("123"));
		verify(preparedStatement).setString(eq(40), eq("123"));
		verify(preparedStatement).setString(eq(41), eq("123"));
		verify(preparedStatement).setString(eq(42), eq("123"));
		verify(preparedStatement).setString(eq(43), eq("123"));
		verify(preparedStatement).setString(eq(44), eq("123"));
		verify(preparedStatement).setString(eq(45), eq("123"));
		verify(preparedStatement).setString(eq(46), eq("123"));
		verify(preparedStatement).setString(eq(47), eq("123"));
		verify(preparedStatement).setString(eq(48), eq("123"));

	}

	@Test(expected = Exception.class)
	public void updateVztDataShareRowWithKeyingFlagFTest_exception() throws Exception {

		PreparedStatement preparedStatement = spy(PreparedStatement.class);

		ValueProvider<String> currentTimestamp = mock(ValueProvider.class);
		when(currentTimestamp.get()).thenReturn("07-07-2020");

		ValueProvider<Integer> batchId = mock(ValueProvider.class);
		when(batchId.get()).thenReturn(80);

		ValueProvider<String> processName = mock(ValueProvider.class);
		when(processName.get()).thenReturn("Verizon CNX Keying");

		Row row = mock(Row.class);
		when(row.getString(eq("KEYING_FLAG"))).thenThrow(new Exception());

		VztDataShareTableHelper.updateVztDataShareRowWithKeyingFlagF(row, preparedStatement, currentTimestamp, batchId,
				processName);
	}

	
	
	@Test
	public void insertVztDataShareRowWithKeyingFlagFTest() throws Exception {

		PreparedStatement preparedStatement = spy(PreparedStatement.class);

		ValueProvider<String> currentTimestamp = mock(ValueProvider.class);
		when(currentTimestamp.get()).thenReturn("07-07-2020");

		ValueProvider<Integer> batchId = mock(ValueProvider.class);
		when(batchId.get()).thenReturn(80);

		ValueProvider<String> processName = mock(ValueProvider.class);
		when(processName.get()).thenReturn("Verizon CNX Keying");

		Row row = mock(Row.class);

		when(row.getString(eq("DATASHARE_ID"))).thenReturn("123");
		when(row.getString(eq("LINE_OF_BUSINESS"))).thenReturn("123");
		when(row.getString(eq("BTN"))).thenReturn("123");
		when(row.getString(eq("BILL_STREET_NO"))).thenReturn("123");
		when(row.getString(eq("BILL_STREET_NAME"))).thenReturn("123");
		when(row.getString(eq("BILL_CITY"))).thenReturn("123");
		when(row.getString(eq("BILL_STATE"))).thenReturn("123");
		when(row.getString(eq("BILL_ZIP"))).thenReturn("123");
		when(row.getString(eq("SVC_STREET_NO"))).thenReturn("123");
		when(row.getString(eq("SVC_STREET_NAME"))).thenReturn("123");
		when(row.getString(eq("SVC_CITY"))).thenReturn("123");
		when(row.getString(eq("SVC_STATE"))).thenReturn("123");
		when(row.getString(eq("SVC_ZIP"))).thenReturn("123");
		when(row.getString(eq("FIRST_NAME"))).thenReturn("123");
		when(row.getString(eq("MIDDLE_NAME"))).thenReturn("123");
		when(row.getString(eq("LAST_NAME"))).thenReturn("123");
		when(row.getString(eq("BUSINESS_NAME"))).thenReturn("123");
		when(row.getString(eq("SSN_TAXID"))).thenReturn("123");
		when(row.getString(eq("ACCOUNT_NO"))).thenReturn("123");
		when(row.getString(eq("LIVE_FINAL_INDICATOR"))).thenReturn("123");
		when(row.getString(eq("SOURCE_BUSINESS"))).thenReturn("123");
		when(row.getString(eq("FIBER_INDICATOR"))).thenReturn("123");
		when(row.getString(eq("KEYING_FLAG"))).thenReturn("F");
		when(row.getString(eq("EFX_SVC_CNX_ID"))).thenReturn("123");
		when(row.getString(eq("EFX_SVC_HHLD_ID"))).thenReturn("123");
		when(row.getString(eq("EFX_SVC_ADDR_ID"))).thenReturn("123");
		when(row.getString(eq("BTN_HMAC"))).thenReturn("123");
		when(row.getString(eq("BILL_STREET_NO_HMAC"))).thenReturn("123");
		when(row.getString(eq("BILL_STREET_NAME_HMAC"))).thenReturn("123");
		when(row.getString(eq("BILL_CITY_HMAC"))).thenReturn("123");
		when(row.getString(eq("BILL_STATE_HMAC"))).thenReturn("123");
		when(row.getString(eq("BILL_ZIP_HMAC"))).thenReturn("123");
		when(row.getString(eq("SVC_STREET_NO_HMAC"))).thenReturn("123");
		when(row.getString(eq("SVC_STREET_NAME_HMAC"))).thenReturn("123");
		when(row.getString(eq("SVC_CITY_HMAC"))).thenReturn("123");
		when(row.getString(eq("SVC_STATE_HMAC"))).thenReturn("123");
		when(row.getString(eq("SVC_ZIP_HMAC"))).thenReturn("123");
		when(row.getString(eq("FIRST_NAME_HMAC"))).thenReturn("123");
		when(row.getString(eq("MIDDLE_NAME_HMAC"))).thenReturn("123");
		when(row.getString(eq("LAST_NAME_HMAC"))).thenReturn("123");
		when(row.getString(eq("BUSINESS_NAME_HMAC"))).thenReturn("123");
		when(row.getString(eq("SSN_TAXID_HMAC"))).thenReturn("123");
		when(row.getString(eq("ACCOUNT_NO_HMAC"))).thenReturn("123");

		VztDataShareTableHelper.insertVztDataShareRowWithKeyingFlagF(row, preparedStatement, currentTimestamp, batchId,
				processName);

		verify(preparedStatement).setString(eq(1), eq("123"));
		verify(preparedStatement).setString(eq(2), eq("123"));
		verify(preparedStatement).setString(eq(3), eq("123"));
		verify(preparedStatement).setString(eq(4), eq("123"));
		verify(preparedStatement).setString(eq(5), eq("123"));
		verify(preparedStatement).setString(eq(6), eq("123"));
		verify(preparedStatement).setString(eq(7), eq("123"));
		verify(preparedStatement).setString(eq(8), eq("123"));
		verify(preparedStatement).setString(eq(9), eq("123"));
		verify(preparedStatement).setString(eq(10), eq("123"));
		verify(preparedStatement).setString(eq(11), eq("123"));
		verify(preparedStatement).setString(eq(12), eq("123"));
		verify(preparedStatement).setString(eq(13), eq("123"));
		verify(preparedStatement).setString(eq(14), eq("123"));
		verify(preparedStatement).setString(eq(15), eq("123"));
		verify(preparedStatement).setString(eq(16), eq("123"));
		verify(preparedStatement).setString(eq(17), eq("123"));
		verify(preparedStatement).setString(eq(18), eq("123"));
		verify(preparedStatement).setString(eq(19), eq("123"));
		verify(preparedStatement).setString(eq(20), eq("123"));
		verify(preparedStatement).setString(eq(21), eq("123"));
		verify(preparedStatement).setString(eq(22), eq("123"));
		verify(preparedStatement).setString(eq(23), eq("F"));
		verify(preparedStatement).setString(eq(24), eq("123"));
		verify(preparedStatement).setString(eq(25), eq("123"));
		verify(preparedStatement).setString(eq(26), eq("123"));
		verify(preparedStatement).setString(eq(28), eq("S"));

		
		verify(preparedStatement).setString(eq(34), eq("123"));
		verify(preparedStatement).setString(eq(35), eq("123"));
		verify(preparedStatement).setString(eq(36), eq("123"));
		verify(preparedStatement).setString(eq(37), eq("123"));
		verify(preparedStatement).setString(eq(38), eq("123"));
		verify(preparedStatement).setString(eq(39), eq("123"));
		verify(preparedStatement).setString(eq(40), eq("123"));
		verify(preparedStatement).setString(eq(41), eq("123"));
		verify(preparedStatement).setString(eq(42), eq("123"));
		verify(preparedStatement).setString(eq(43), eq("123"));
		verify(preparedStatement).setString(eq(44), eq("123"));
		verify(preparedStatement).setString(eq(45), eq("123"));
		verify(preparedStatement).setString(eq(46), eq("123"));
		verify(preparedStatement).setString(eq(47), eq("123"));
		verify(preparedStatement).setString(eq(48), eq("123"));
		verify(preparedStatement).setString(eq(49), eq("123"));
		verify(preparedStatement).setString(eq(50), eq("123"));

	}

	@Test(expected = Exception.class)
	public void insertVztDataShareRowWithKeyingFlagFTest_exception() throws Exception {

		PreparedStatement preparedStatement = spy(PreparedStatement.class);

		ValueProvider<String> currentTimestamp = mock(ValueProvider.class);
		when(currentTimestamp.get()).thenReturn("07-07-2020");

		ValueProvider<Integer> batchId = mock(ValueProvider.class);
		when(batchId.get()).thenReturn(80);

		ValueProvider<String> processName = mock(ValueProvider.class);
		when(processName.get()).thenReturn("Verizon CNX Keying");

		Row row = mock(Row.class);
		when(row.getString(eq("KEYING_FLAG"))).thenThrow(new Exception());

		VztDataShareTableHelper.insertVztDataShareRowWithKeyingFlagF(row, preparedStatement, currentTimestamp, batchId,
				processName);
	}
	
	
	
	@Test
	public void updateVztDataShareRowWithKeyingFlagTTest() throws Exception {

		
			PreparedStatement preparedStatement = spy(PreparedStatement.class);

			ValueProvider<String> currentTimestamp = mock(ValueProvider.class);
			when(currentTimestamp.get()).thenReturn("07-07-2020");

			ValueProvider<Integer> batchId = mock(ValueProvider.class);
			when(batchId.get()).thenReturn(80);

			ValueProvider<String> processName = mock(ValueProvider.class);
			when(processName.get()).thenReturn("Verizon CNX Keying");

			Row row = mock(Row.class);

			when(row.getString(eq("LINE_OF_BUSINESS"))).thenReturn("123");
			when(row.getString(eq("BTN"))).thenReturn("123");
			when(row.getString(eq("BILL_STREET_NO"))).thenReturn("123");
			when(row.getString(eq("BILL_STREET_NAME"))).thenReturn("123");
			when(row.getString(eq("BILL_CITY"))).thenReturn("123");
			when(row.getString(eq("BILL_STATE"))).thenReturn("123");
			when(row.getString(eq("BILL_ZIP"))).thenReturn("123");
			when(row.getString(eq("SVC_STREET_NO"))).thenReturn("123");
			when(row.getString(eq("SVC_STREET_NAME"))).thenReturn("123");
			when(row.getString(eq("SVC_CITY"))).thenReturn("123");
			when(row.getString(eq("SVC_STATE"))).thenReturn("123");
			when(row.getString(eq("SVC_ZIP"))).thenReturn("123");
			when(row.getString(eq("FIRST_NAME"))).thenReturn("123");
			when(row.getString(eq("MIDDLE_NAME"))).thenReturn("123");
			when(row.getString(eq("LAST_NAME"))).thenReturn("123");
			when(row.getString(eq("BUSINESS_NAME"))).thenReturn("123");
			when(row.getString(eq("SSN_TAXID"))).thenReturn("123");
			when(row.getString(eq("ACCOUNT_NO"))).thenReturn("123");
			when(row.getString(eq("LIVE_FINAL_INDICATOR"))).thenReturn("123");
			when(row.getString(eq("SOURCE_BUSINESS"))).thenReturn("123");
			when(row.getString(eq("FIBER_INDICATOR"))).thenReturn("123");
			when(row.getString(eq("KEYING_FLAG"))).thenReturn("T");
			
			when(row.getString(eq("BTN_HMAC"))).thenReturn("123");
			when(row.getString(eq("BILL_STREET_NO_HMAC"))).thenReturn("123");
			when(row.getString(eq("BILL_STREET_NAME_HMAC"))).thenReturn("123");
			when(row.getString(eq("BILL_CITY_HMAC"))).thenReturn("123");
			when(row.getString(eq("BILL_STATE_HMAC"))).thenReturn("123");
			when(row.getString(eq("BILL_ZIP_HMAC"))).thenReturn("123");
			when(row.getString(eq("SVC_STREET_NO_HMAC"))).thenReturn("123");
			when(row.getString(eq("SVC_STREET_NAME_HMAC"))).thenReturn("123");
			when(row.getString(eq("SVC_CITY_HMAC"))).thenReturn("123");
			when(row.getString(eq("SVC_STATE_HMAC"))).thenReturn("123");
			when(row.getString(eq("SVC_ZIP_HMAC"))).thenReturn("123");
			when(row.getString(eq("FIRST_NAME_HMAC"))).thenReturn("123");
			when(row.getString(eq("MIDDLE_NAME_HMAC"))).thenReturn("123");
			when(row.getString(eq("LAST_NAME_HMAC"))).thenReturn("123");
			when(row.getString(eq("BUSINESS_NAME_HMAC"))).thenReturn("123");
			when(row.getString(eq("SSN_TAXID_HMAC"))).thenReturn("123");
			when(row.getString(eq("ACCOUNT_NO_HMAC"))).thenReturn("123");
			when(row.getString(eq("DATASHARE_ID"))).thenReturn("123");

			VztDataShareTableHelper.updateVztDataShareRowWithKeyingFlagT(row, preparedStatement, currentTimestamp, batchId,
					processName);

			verify(preparedStatement).setString(eq(1), eq("123"));
			verify(preparedStatement).setString(eq(2), eq("123"));
			verify(preparedStatement).setString(eq(3), eq("123"));
			verify(preparedStatement).setString(eq(4), eq("123"));
			verify(preparedStatement).setString(eq(5), eq("123"));
			verify(preparedStatement).setString(eq(6), eq("123"));
			verify(preparedStatement).setString(eq(7), eq("123"));
			verify(preparedStatement).setString(eq(8), eq("123"));
			verify(preparedStatement).setString(eq(9), eq("123"));
			verify(preparedStatement).setString(eq(10), eq("123"));
			verify(preparedStatement).setString(eq(11), eq("123"));
			verify(preparedStatement).setString(eq(12), eq("123"));
			verify(preparedStatement).setString(eq(13), eq("123"));
			verify(preparedStatement).setString(eq(14), eq("123"));
			verify(preparedStatement).setString(eq(15), eq("123"));
			verify(preparedStatement).setString(eq(16), eq("123"));
			verify(preparedStatement).setString(eq(17), eq("123"));
			verify(preparedStatement).setString(eq(18), eq("123"));
			verify(preparedStatement).setString(eq(19), eq("123"));
			verify(preparedStatement).setString(eq(20), eq("123"));
			verify(preparedStatement).setString(eq(21), eq("123"));
			verify(preparedStatement).setString(eq(22), eq("T"));
			
			verify(preparedStatement).setString(eq(26), eq("123"));
			verify(preparedStatement).setString(eq(27), eq("123"));
			verify(preparedStatement).setString(eq(28), eq("123"));
			verify(preparedStatement).setString(eq(29), eq("123"));
			verify(preparedStatement).setString(eq(30), eq("123"));
			verify(preparedStatement).setString(eq(31), eq("123"));
			verify(preparedStatement).setString(eq(32), eq("123"));
			verify(preparedStatement).setString(eq(33), eq("123"));
			verify(preparedStatement).setString(eq(34), eq("123"));
			verify(preparedStatement).setString(eq(35), eq("123"));
			verify(preparedStatement).setString(eq(36), eq("123"));
			verify(preparedStatement).setString(eq(37), eq("123"));
			verify(preparedStatement).setString(eq(38), eq("123"));
			verify(preparedStatement).setString(eq(39), eq("123"));
			verify(preparedStatement).setString(eq(40), eq("123"));
			verify(preparedStatement).setString(eq(41), eq("123"));
			verify(preparedStatement).setString(eq(42), eq("123"));
			verify(preparedStatement).setString(eq(43), eq("123"));
			

		}

		@Test(expected = Exception.class)
		public void updateVztDataShareRowWithKeyingFlagT_exception() throws Exception {

			PreparedStatement preparedStatement = spy(PreparedStatement.class);

			ValueProvider<String> currentTimestamp = mock(ValueProvider.class);
			when(currentTimestamp.get()).thenReturn("07-07-2020");

			ValueProvider<Integer> batchId = mock(ValueProvider.class);
			when(batchId.get()).thenReturn(80);

			ValueProvider<String> processName = mock(ValueProvider.class);
			when(processName.get()).thenReturn("Verizon CNX Keying");

			Row row = mock(Row.class);
			when(row.getString(eq("KEYING_FLAG"))).thenThrow(new Exception());

			VztDataShareTableHelper.updateVztDataShareRowWithKeyingFlagT(row, preparedStatement, currentTimestamp, batchId,
					processName);
		}
		
		@Test
		public void insertVztDataShareRowWithKeyingFlagTTest() throws Exception {

			
				PreparedStatement preparedStatement = spy(PreparedStatement.class);

				ValueProvider<String> currentTimestamp = mock(ValueProvider.class);
				when(currentTimestamp.get()).thenReturn("07-07-2020");

				ValueProvider<Integer> batchId = mock(ValueProvider.class);
				when(batchId.get()).thenReturn(80);

				ValueProvider<String> processName = mock(ValueProvider.class);
				when(processName.get()).thenReturn("Verizon CNX Keying");

				Row row = mock(Row.class);

				when(row.getString(eq("DATASHARE_ID"))).thenReturn("123");
				when(row.getString(eq("LINE_OF_BUSINESS"))).thenReturn("123");
				when(row.getString(eq("BTN"))).thenReturn("123");
				when(row.getString(eq("BILL_STREET_NO"))).thenReturn("123");
				when(row.getString(eq("BILL_STREET_NAME"))).thenReturn("123");
				when(row.getString(eq("BILL_CITY"))).thenReturn("123");
				when(row.getString(eq("BILL_STATE"))).thenReturn("123");
				when(row.getString(eq("BILL_ZIP"))).thenReturn("123");
				when(row.getString(eq("SVC_STREET_NO"))).thenReturn("123");
				when(row.getString(eq("SVC_STREET_NAME"))).thenReturn("123");
				when(row.getString(eq("SVC_CITY"))).thenReturn("123");
				when(row.getString(eq("SVC_STATE"))).thenReturn("123");
				when(row.getString(eq("SVC_ZIP"))).thenReturn("123");
				when(row.getString(eq("FIRST_NAME"))).thenReturn("123");
				when(row.getString(eq("MIDDLE_NAME"))).thenReturn("123");
				when(row.getString(eq("LAST_NAME"))).thenReturn("123");
				when(row.getString(eq("BUSINESS_NAME"))).thenReturn("123");
				when(row.getString(eq("SSN_TAXID"))).thenReturn("123");
				when(row.getString(eq("ACCOUNT_NO"))).thenReturn("123");
				when(row.getString(eq("LIVE_FINAL_INDICATOR"))).thenReturn("123");
				when(row.getString(eq("SOURCE_BUSINESS"))).thenReturn("123");
				when(row.getString(eq("FIBER_INDICATOR"))).thenReturn("123");
				when(row.getString(eq("KEYING_FLAG"))).thenReturn("T");
				
				when(row.getString(eq("BTN_HMAC"))).thenReturn("123");
				when(row.getString(eq("BILL_STREET_NO_HMAC"))).thenReturn("123");
				when(row.getString(eq("BILL_STREET_NAME_HMAC"))).thenReturn("123");
				when(row.getString(eq("BILL_CITY_HMAC"))).thenReturn("123");
				when(row.getString(eq("BILL_STATE_HMAC"))).thenReturn("123");
				when(row.getString(eq("BILL_ZIP_HMAC"))).thenReturn("123");
				when(row.getString(eq("SVC_STREET_NO_HMAC"))).thenReturn("123");
				when(row.getString(eq("SVC_STREET_NAME_HMAC"))).thenReturn("123");
				when(row.getString(eq("SVC_CITY_HMAC"))).thenReturn("123");
				when(row.getString(eq("SVC_STATE_HMAC"))).thenReturn("123");
				when(row.getString(eq("SVC_ZIP_HMAC"))).thenReturn("123");
				when(row.getString(eq("FIRST_NAME_HMAC"))).thenReturn("123");
				when(row.getString(eq("MIDDLE_NAME_HMAC"))).thenReturn("123");
				when(row.getString(eq("LAST_NAME_HMAC"))).thenReturn("123");
				when(row.getString(eq("BUSINESS_NAME_HMAC"))).thenReturn("123");
				when(row.getString(eq("SSN_TAXID_HMAC"))).thenReturn("123");
				when(row.getString(eq("ACCOUNT_NO_HMAC"))).thenReturn("123");

				VztDataShareTableHelper.insertVztDataShareRowWithKeyingFlagT(row, preparedStatement, currentTimestamp, batchId,
						processName);

				verify(preparedStatement).setString(eq(1), eq("123"));
				verify(preparedStatement).setString(eq(2), eq("123"));
				verify(preparedStatement).setString(eq(3), eq("123"));
				verify(preparedStatement).setString(eq(4), eq("123"));
				verify(preparedStatement).setString(eq(5), eq("123"));
				verify(preparedStatement).setString(eq(6), eq("123"));
				verify(preparedStatement).setString(eq(7), eq("123"));
				verify(preparedStatement).setString(eq(8), eq("123"));
				verify(preparedStatement).setString(eq(9), eq("123"));
				verify(preparedStatement).setString(eq(10), eq("123"));
				verify(preparedStatement).setString(eq(11), eq("123"));
				verify(preparedStatement).setString(eq(12), eq("123"));
				verify(preparedStatement).setString(eq(13), eq("123"));
				verify(preparedStatement).setString(eq(14), eq("123"));
				verify(preparedStatement).setString(eq(15), eq("123"));
				verify(preparedStatement).setString(eq(16), eq("123"));
				verify(preparedStatement).setString(eq(17), eq("123"));
				verify(preparedStatement).setString(eq(18), eq("123"));
				verify(preparedStatement).setString(eq(19), eq("123"));
				verify(preparedStatement).setString(eq(20), eq("123"));
				verify(preparedStatement).setString(eq(21), eq("123"));
				verify(preparedStatement).setString(eq(22), eq("123"));
				
				verify(preparedStatement).setString(eq(23), eq("T"));
				
				verify(preparedStatement).setString(eq(29), eq("123"));
				verify(preparedStatement).setString(eq(30), eq("123"));
				verify(preparedStatement).setString(eq(31), eq("123"));
				verify(preparedStatement).setString(eq(32), eq("123"));
				verify(preparedStatement).setString(eq(33), eq("123"));
				verify(preparedStatement).setString(eq(34), eq("123"));
				verify(preparedStatement).setString(eq(35), eq("123"));
				verify(preparedStatement).setString(eq(36), eq("123"));
				verify(preparedStatement).setString(eq(37), eq("123"));
				verify(preparedStatement).setString(eq(38), eq("123"));
				verify(preparedStatement).setString(eq(39), eq("123"));
				verify(preparedStatement).setString(eq(40), eq("123"));
				verify(preparedStatement).setString(eq(41), eq("123"));
				verify(preparedStatement).setString(eq(42), eq("123"));
				verify(preparedStatement).setString(eq(43), eq("123"));
				verify(preparedStatement).setString(eq(44), eq("123"));
				verify(preparedStatement).setString(eq(45), eq("123"));
				

			}

			@Test(expected = Exception.class)
			public void insertVztDataShareRowWithKeyingFlagT_exception() throws Exception {

				PreparedStatement preparedStatement = spy(PreparedStatement.class);

				ValueProvider<String> currentTimestamp = mock(ValueProvider.class);
				when(currentTimestamp.get()).thenReturn("07-07-2020");

				ValueProvider<Integer> batchId = mock(ValueProvider.class);
				when(batchId.get()).thenReturn(80);

				ValueProvider<String> processName = mock(ValueProvider.class);
				when(processName.get()).thenReturn("Verizon CNX Keying");

				Row row = mock(Row.class);
				when(row.getString(eq("KEYING_FLAG"))).thenThrow(new Exception());

				VztDataShareTableHelper.updateVztDataShareRowWithKeyingFlagT(row, preparedStatement, currentTimestamp, batchId,
						processName);
			}


}
