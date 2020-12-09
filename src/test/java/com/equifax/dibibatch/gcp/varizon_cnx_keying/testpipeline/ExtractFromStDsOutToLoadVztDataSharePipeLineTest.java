package com.equifax.dibibatch.gcp.varizon_cnx_keying.testpipeline;

import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ExtractFromStDsOutToLoadVztDataSharePipeLineTest {

	@Rule
	public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

	static final String[] args = new String[] { "--runner=DirectRunner" };

	// Smoke Test
	@Test
	public void loadCmfDataTest() throws Exception {
		
	}
}
