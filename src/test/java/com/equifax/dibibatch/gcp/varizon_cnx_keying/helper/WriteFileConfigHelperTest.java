package com.equifax.dibibatch.gcp.varizon_cnx_keying.helper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;

import org.apache.beam.sdk.options.PipelineOptions.CheckEnabled;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.values.Row;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.equifax.usis.dibi.batch.gcp.dataflow.common.io.PGPEncryptFileIO.FieldFn;

@RunWith(JUnit4.class)
public class WriteFileConfigHelperTest {

	@Rule
	public final TemporaryFolder folder = new TemporaryFolder();

	@Rule
	public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

	ValueProvider<String> outputPath;

	@Before
	public void setUp() throws IOException {

		outputPath = StaticValueProvider.of(folder.newFolder().getPath());

		p.getOptions().getStableUniqueNames();
		p.getOptions().setStableUniqueNames(CheckEnabled.WARNING);

	}

	@Test
	public void getConfigForExtractDICNX0000002EfxVzwFile() {

		@SuppressWarnings("rawtypes")
		ProcessContext c = mock(ProcessContext.class);
		Row row = mock(Row.class);

		when(row.getString(anyString())).thenReturn("test");

		when(c.element()).thenReturn(row);
		Map<String, FieldFn<Row>> config = WriteFileConfigHelper.getConfigForExtractDICNX0000002EfxVzwFile();

		assertEquals(14, config.keySet().size());

	}

	@Test
	public void getConfigForExtractVzDsFuzzyReqFile() {

		@SuppressWarnings("rawtypes")
		ProcessContext c = mock(ProcessContext.class);
		Row row = mock(Row.class);

		when(row.getString(anyString())).thenReturn("test");

		when(c.element()).thenReturn(row);
		Map<String, FieldFn<Row>> config = WriteFileConfigHelper.getConfigForExtractVzDsFuzzyReqFile();

		assertEquals(8, config.keySet().size());
	}

	@Test
	public void getConfigForExtractInactivatedOverrideRecordsFile() {

		@SuppressWarnings("rawtypes")
		ProcessContext c = mock(ProcessContext.class);
		Row row = mock(Row.class);

		when(row.getString(anyString())).thenReturn("test");
		when(c.element()).thenReturn(row);
		Map<String, FieldFn<Row>> config = WriteFileConfigHelper.getConfigForExtractInactivatedOverrideRecordsFile();

		for (Map.Entry<String, FieldFn<Row>> entry : config.entrySet()) {

			FieldFn<Row> fcn = entry.getValue();

			@SuppressWarnings("unchecked")
			String result = (String) fcn.apply(c);

			assertThat(result, Matchers.any(String.class));
		}

	}

	@Test
	public void getCnxOverrideFile() {

		@SuppressWarnings("rawtypes")
		ProcessContext c = mock(ProcessContext.class);
		Row row = mock(Row.class);

		when(row.getString(anyString())).thenReturn("test");
		when(c.element()).thenReturn(row);
		Map<String, FieldFn<Row>> config = WriteFileConfigHelper.getCnxOverrideFile();

		assertEquals(6, config.keySet().size());
	}

	@Test
	public void getCnxIdDeltaFile() {

		@SuppressWarnings("rawtypes")
		ProcessContext c = mock(ProcessContext.class);
		Row row = mock(Row.class);

		when(row.getString(anyString())).thenReturn("test");
		when(c.element()).thenReturn(row);
		Map<String, FieldFn<Row>> config = WriteFileConfigHelper.getCnxIdDeltaFile();

		assertEquals(7, config.keySet().size());

	}

	@Test
	public void getConfigForExtractDivzcommsR00VztDsProdKrspF() {

		@SuppressWarnings("rawtypes")
		ProcessContext c = mock(ProcessContext.class);
		Row row = mock(Row.class);

		when(row.getString(anyString())).thenReturn("test");
		when(c.element()).thenReturn(row);
		Map<String, FieldFn<Row>> config = WriteFileConfigHelper.getConfigForExtractDivzcommsR00VztDsProdKrspF();

		assertEquals(11, config.keySet().size());

	}

}
