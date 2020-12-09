package com.equifax.dibibatch.gcp.varizon_cnx_keying.transforms;

import java.io.IOException;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.ExtractDataShareSsaConInputSchema;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.ExtractVzDsFuzzyReqExtractSchema;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.exception.InvalidFieldException;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.BarricaderInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.Barricader;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.ObjectUtil;

public class VzDsFuzzyReqExtractDecryptTransformation extends DoFn<Row, Row> {

	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggerFactory.getLogger(VzDataShareCnxRepoDecryptTransformation.class);

	BarricaderInfo barricaderInfo = null;
	Barricader barricader = null;

	@StartBundle
	public void startBundle() throws Exception {
		barricader = Barricader.create().withInfo(barricaderInfo).andBuild();
	}

	public VzDsFuzzyReqExtractDecryptTransformation(BarricaderInfo barricaderInfo) {
		this.barricaderInfo = barricaderInfo;
	}

	@ProcessElement
	public void processElement(ProcessContext c) {

		try {

			Row row = buildRowWithSchema(ExtractVzDsFuzzyReqExtractSchema.ExtractVzDsFuzzyReqExtractToDecrypt(),
					c.element());
			c.output(row);

		} catch (Exception ex) {

			log.error("Error while processing input", ex);
		}
	}

	private Row buildRowWithSchema(Schema schema, Row inputRow) throws IOException {

		Row.Builder rowBuilder = Row.withSchema(schema);

		String SURROGATE_KEY = (ObjectUtil.checkForNull(inputRow.getString("SURROGATE_KEY")));
		String FIRST_NAME =(ObjectUtil.checkForNull(inputRow.getString("FIRST_NAME")));
		String MIDDLE_NAME =(ObjectUtil.checkForNull(inputRow.getString("MIDDLE_NAME")));
		String LAST_NAME =(ObjectUtil.checkForNull(inputRow.getString("LAST_NAME")));
		String BILL_STREET_NO =(ObjectUtil.checkForNull(inputRow.getString("BILL_STREET_NO")));
		String BILL_STREET_NAME =(ObjectUtil.checkForNull(inputRow.getString("BILL_STREET_NAME")));
		String BILL_CITY =(ObjectUtil.checkForNull(inputRow.getString("BILL_CITY")));
		String BILL_STATE =(ObjectUtil.checkForNull(inputRow.getString("BILL_STATE")));
		String BILL_ZIP =(ObjectUtil.checkForNull(inputRow.getString("BILL_ZIP")));
		String SVC_STREET_NO =(ObjectUtil.checkForNull(inputRow.getString("SVC_STREET_NO")));
		String SVC_STREET_NAME =(ObjectUtil.checkForNull(inputRow.getString("SVC_STREET_NAME")));
		String SVC_CITY =(ObjectUtil.checkForNull(inputRow.getString("SVC_CITY")));
		String SVC_STATE =(ObjectUtil.checkForNull(inputRow.getString("SVC_STATE")));
		String SVC_ZIP =(ObjectUtil.checkForNull(inputRow.getString("SVC_ZIP")));
				
		
		 FIRST_NAME =barricader.decryptIgnoreEmpty(FIRST_NAME);
		 MIDDLE_NAME =barricader.decryptIgnoreEmpty(MIDDLE_NAME);
		 LAST_NAME =barricader.decryptIgnoreEmpty(LAST_NAME);
		 BILL_STREET_NO =barricader.decryptIgnoreEmpty(BILL_STREET_NO);
		 BILL_STREET_NAME =barricader.decryptIgnoreEmpty(BILL_STREET_NAME);
		 BILL_CITY =barricader.decryptIgnoreEmpty(BILL_CITY);
		 BILL_STATE =barricader.decryptIgnoreEmpty(BILL_STATE);
		 BILL_ZIP =barricader.decryptIgnoreEmpty(BILL_ZIP);
		 SVC_STREET_NO =barricader.decryptIgnoreEmpty(SVC_STREET_NO);
		 SVC_STREET_NAME =barricader.decryptIgnoreEmpty(SVC_STREET_NAME);
		 SVC_CITY =barricader.decryptIgnoreEmpty(SVC_CITY);
		 SVC_STATE =barricader.decryptIgnoreEmpty(SVC_STATE);
		 SVC_ZIP =barricader.decryptIgnoreEmpty(SVC_ZIP);
		 

		 
		
		rowBuilder.addValue(SURROGATE_KEY);
		rowBuilder.addValue(FIRST_NAME);
		rowBuilder.addValue(MIDDLE_NAME);
		rowBuilder.addValue(LAST_NAME);
		rowBuilder.addValue(BILL_STREET_NO);
		rowBuilder.addValue(BILL_STREET_NAME);
		rowBuilder.addValue(BILL_CITY);
		rowBuilder.addValue(BILL_STATE);
		rowBuilder.addValue(BILL_ZIP);
		rowBuilder.addValue(SVC_STREET_NO);
		rowBuilder.addValue(SVC_STREET_NAME);
		rowBuilder.addValue(SVC_CITY);
		rowBuilder.addValue(SVC_STATE);
		rowBuilder.addValue(SVC_ZIP);

		return rowBuilder.build();
		
	}

	private void validate(Row row) throws InvalidFieldException {

	}
}
