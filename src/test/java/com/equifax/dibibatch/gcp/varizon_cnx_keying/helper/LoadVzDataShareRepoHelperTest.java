package com.equifax.dibibatch.gcp.varizon_cnx_keying.helper;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.helper.LoadVzDataShareRepoHelper;

public class LoadVzDataShareRepoHelperTest {

	@Test
	public void deleteVzDataShareRepoTableRow_success() throws Exception {

		PreparedStatement preparedStatement = spy(PreparedStatement.class);

		Row row = mock(Row.class);
		when(row.getDecimal(eq("VZ_DS_FK_ID"))).thenReturn(BigDecimal.valueOf(12345));

		LoadVzDataShareRepoHelper.deleteVzDataShareRepoTableRow(row, preparedStatement);

		verify(preparedStatement).setBigDecimal(eq(1), eq(BigDecimal.valueOf(12345)));
	}

	@Test(expected = Exception.class)
	public void deleteVzDataShareRepoTableRow_exception() throws Exception {

		PreparedStatement preparedStatement = spy(PreparedStatement.class);

		Row row = mock(Row.class);
		when(row.getDecimal(eq("VZ_DS_FK_ID"))).thenThrow(new Exception());

		LoadVzDataShareRepoHelper.deleteVzDataShareRepoTableRow(row, preparedStatement);
	}

}
