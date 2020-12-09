package com.equifax.dibibatch.gcp.varizon_cnx_keying.helper;

import static org.mockito.Mockito.*;

import java.sql.PreparedStatement;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.helper.LoadFromInactiveOvrerrideRecordsHelper;

public class LoadFromInactiveOvrerrideRecordsHelperTest {

	@Test
	public void updateVztDataShareRowTest_success() throws Exception {

		PreparedStatement preparedStatement = spy(PreparedStatement.class);

		ValueProvider<String> currentTimestamp = mock(ValueProvider.class);
		when(currentTimestamp.get()).thenReturn("07-07-2020");

		Row row = mock(Row.class);
		when(row.getString(eq("DATASHARE_ID"))).thenReturn("123");

		LoadFromInactiveOvrerrideRecordsHelper.updateVztDataShareRow(row, preparedStatement, currentTimestamp);

		verify(preparedStatement).setString(eq(1), eq("Inactive"));
		verify(preparedStatement).setString(eq(2), eq("123"));
	}

	@Test(expected = Exception.class)
	public void updateVztDataShareRowTest_exception() throws Exception {

		PreparedStatement preparedStatement = spy(PreparedStatement.class);

		ValueProvider<String> currentTimestamp = mock(ValueProvider.class);
		when(currentTimestamp.get()).thenReturn("07-07-2020");

		Row row = mock(Row.class);
		when(row.getString(eq("DATASHARE_ID"))).thenThrow(new Exception());

		LoadFromInactiveOvrerrideRecordsHelper.updateVztDataShareRow(row, preparedStatement, currentTimestamp);
	}

	@Test
	public void updateConnexusKeyOverrideRowTest_success() throws Exception {

		PreparedStatement preparedStatement = spy(PreparedStatement.class);

		ValueProvider<String> currentTimestamp = mock(ValueProvider.class);
		when(currentTimestamp.get()).thenReturn("07-07-2020");

		Row row = mock(Row.class);
		when(row.getString(eq("DATASHARE_ID"))).thenReturn("78909");

		LoadFromInactiveOvrerrideRecordsHelper.updateConnexusKeyOverrideRow(row, preparedStatement, currentTimestamp);

		verify(preparedStatement).setString(eq(1), eq("CNX"));
		verify(preparedStatement).setString(eq(2), eq("Inactive"));
		verify(preparedStatement).setString(eq(4), eq("78909"));
	}

	@Test(expected = Exception.class)
	public void updateConnexusKeyOverrideRowTest_exception() throws Exception {

		PreparedStatement preparedStatement = spy(PreparedStatement.class);

		ValueProvider<String> currentTimestamp = mock(ValueProvider.class);
		when(currentTimestamp.get()).thenReturn("07-07-2020");

		Row row = mock(Row.class);
		when(row.getString(eq("DATASHARE_ID"))).thenThrow(new Exception());

		LoadFromInactiveOvrerrideRecordsHelper.updateConnexusKeyOverrideRow(row, preparedStatement, currentTimestamp);
	}
}
