package com.equifax.dibibatch.gcp.varizon_cnx_keying.transforms;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.DoFn.StartBundle;
import org.apache.beam.sdk.values.Row;
import org.joda.time.ReadableDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.ExtractDataShareSsaConInputSchema;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.LoadVztDataShareCnxRepoFromVwVztDataShareSchema;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.exception.InvalidFieldException;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.BarricaderInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.Barricader;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.ObjectUtil;

public class DataShareSsaConInputTransformation extends DoFn<Row, Row> {

	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggerFactory.getLogger(VzDataShareCnxRepoDecryptTransformation.class);

	BarricaderInfo barricaderInfo = null;
	Barricader barricader = null;

	@StartBundle
	public void startBundle() throws Exception {
		barricader = Barricader.create().withInfo(barricaderInfo).andBuild();
	}

	public DataShareSsaConInputTransformation(BarricaderInfo barricaderInfo) {
		this.barricaderInfo = barricaderInfo;
	}

	@ProcessElement
	public void processElement(ProcessContext c) {

		try {

			Row row = buildRowWithSchema(ExtractDataShareSsaConInputSchema.ExtractExtractDataShareSsaConInputbeforeDecrypt(),
					c.element());
			c.output(row);

		} catch (Exception ex) {

			log.error("Error while processing input", ex);
		}
	}

	private Row buildRowWithSchema(Schema schema, Row inputRow) throws IOException {

		Row.Builder rowBuilder = Row.withSchema(schema);
		


		
		 String FIRST_NAME = (ObjectUtil.checkForNull(inputRow.getString("FIRST_NAME")));
		 String LAST_NAME = (ObjectUtil.checkForNull(inputRow.getString("LAST_NAME")));
		 
		 		 
		BigDecimal EFX_UNIQ_ID = (inputRow.getDecimal("EFX_UNIQ_ID"));
		String EFX_NM = (ObjectUtil.checkForNull(FIRST_NAME + " " + LAST_NAME));// EFX_NM
		

		EFX_NM = barricader.decryptIgnoreEmpty(EFX_NM);
		//EFX_ADDR = barricader.decryptIgnoreEmpty(EFX_ADDR);

		
		
		
		rowBuilder.addValue(EFX_UNIQ_ID);
		rowBuilder.addValue(EFX_NM);
		//rowBuilder.addValue(EFX_ADDR);
		
		return rowBuilder.build();
	}

	private void validate(Row row) throws InvalidFieldException {

	}

}