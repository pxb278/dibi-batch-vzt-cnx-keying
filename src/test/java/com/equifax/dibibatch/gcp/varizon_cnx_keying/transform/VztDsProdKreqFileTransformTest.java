package com.equifax.dibibatch.gcp.varizon_cnx_keying.transform;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.CustomSchema;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.transforms.VztDsProdKreqFileTransform;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.exception.InvalidFieldException;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.BarricaderInfo;

	@RunWith(JUnit4.class)
	public class VztDsProdKreqFileTransformTest {
		
		static final TupleTag<String> invalidInputRecordsTag = new TupleTag<String>() {
		};
		static final TupleTag<Row> validInputRecordsTag = new TupleTag<Row>() {
		};
		
		@Rule
		public TestPipeline testPipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
		
		@Test//(expected = UnsupportedOperationException.class)
		public void validationTest() throws InvalidFieldException {
			BarricaderInfo barricaderInfo = BarricaderInfo.create().withEncryption(StaticValueProvider.of(false));
			
			List<String> fileRow = Arrays.asList("390941882|C|9999999999|100|CENTER GROVE RD APT/STE 4|RNDLPH TWP|NJ|07869|100|CENTER GROVE RD BLDG 7 APT/STE 4|RNDLPH TWP|NJ|07869|CASSADY||SHOAFF||490048439|5561111450001|F|VBB|NA|F|623306353109|106098279310|354282918804|20191127|678765");
			
			PCollectionTuple tupleTag = testPipeline
					.apply("Create Pcollection Of Row", Create.of(fileRow).withCoder(NullableCoder.of(StringUtf8Coder.of())))										
					.apply("Barricade Encrypt",	ParDo.of(new VztDsProdKreqFileTransform(barricaderInfo,validInputRecordsTag,invalidInputRecordsTag))
					.withOutputTags(validInputRecordsTag, TupleTagList.of(invalidInputRecordsTag)));
			
			PCollection<Row> rowsData = tupleTag.get(validInputRecordsTag.getId());
			rowsData.setRowSchema(CustomSchema.extrDataForVztDataShareTable());
			
			PCollection<String> valueOf = rowsData.apply("Convert to String", ParDo.of(new getValueOf()));
			testPipeline.run();
		}
		static class getValueOf extends DoFn<Row, String> {
			private static final long serialVersionUID = 1L;
			@ProcessElement
			public void process(ProcessContext c) {
				Row row = c.element();
				c.output(row.getString("DATASHARE_ID"));
			}
		}


}
