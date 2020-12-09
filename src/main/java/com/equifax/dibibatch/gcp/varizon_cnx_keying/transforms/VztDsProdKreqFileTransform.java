package com.equifax.dibibatch.gcp.varizon_cnx_keying.transforms;

import java.io.IOException;
import java.util.Iterator;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.CustomSchema;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.exception.InvalidFieldException;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.BarricaderInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.Barricader;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.ObjectUtil;
import com.google.common.base.Splitter;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class VztDsProdKreqFileTransform extends DoFn<String, Row> {

	private static final long serialVersionUID = -6774172041562294147L;

	private static final Logger log = LoggerFactory.getLogger(VztDsProdKreqFileTransform.class);

	

	BarricaderInfo barricaderInfo = null;
	Barricader barricader = null;

	TupleTag validRowsTag;
	TupleTag invalidRowsTag;

	@StartBundle
	public void startBundle() throws Exception {

		barricader = Barricader.create().withInfo(barricaderInfo).andBuild();
	}
	
	
	public VztDsProdKreqFileTransform(BarricaderInfo barricaderInfo, TupleTag validRowsTag, TupleTag invalidRowsTag) {
		    this.barricaderInfo = barricaderInfo;
			this.validRowsTag = validRowsTag;
			this.invalidRowsTag = invalidRowsTag;
		}

	@ProcessElement
	public void processElement(ProcessContext c) {
		try {

			Row row = buildRowWithSchema(CustomSchema.extrDataForVztDataShareTable(), c.element());

			validate(row);

			c.output(this.validRowsTag, row);

		} catch (Exception ex) {

			log.error("Exception occurred for: " + c.element(), ex);

			c.output(this.invalidRowsTag, c.element() + "|" + ex.getMessage());
		}
	}

	private Row buildRowWithSchema(Schema schema, String input) throws IOException {

		
		/* if(input.trim().contains("DataShare Batch Request File")) { return null; } */
		 
		Row.Builder rowBuilder = Row.withSchema(schema);

		Iterator<String> rowSplitter = Splitter.on("|").trimResults().split(input).iterator();
		

		String DATASHARE_ID= ObjectUtil.checkForNull(rowSplitter.next());
		String LINE_OF_BUSINESS= ObjectUtil.checkForNull(rowSplitter.next());
		String BTN= ObjectUtil.checkForNull(rowSplitter.next());
		String BILL_STREET_NO= ObjectUtil.checkForNull(rowSplitter.next());
		String BILL_STREET_NAME= ObjectUtil.checkForNull(rowSplitter.next());
		String BILL_CITY= ObjectUtil.checkForNull(rowSplitter.next());
		String BILL_STATE= ObjectUtil.checkForNull(rowSplitter.next());
		String BILL_ZIP= ObjectUtil.checkForNull(rowSplitter.next());
		String SVC_STREET_NO= ObjectUtil.checkForNull(rowSplitter.next());
		String SVC_STREET_NAME= ObjectUtil.checkForNull(rowSplitter.next());
		String SVC_CITY= ObjectUtil.checkForNull(rowSplitter.next());
		String SVC_STATE= ObjectUtil.checkForNull(rowSplitter.next());
		String SVC_ZIP= ObjectUtil.checkForNull(rowSplitter.next());
		String FIRST_NAME= ObjectUtil.checkForNull(rowSplitter.next());
		String MIDDLE_NAME= ObjectUtil.checkForNull(rowSplitter.next());
		String LAST_NAME= ObjectUtil.checkForNull(rowSplitter.next());
		String BUSINESS_NAME= ObjectUtil.checkForNull(rowSplitter.next());
		String SSN_TAXID= ObjectUtil.checkForNull(rowSplitter.next());
		String ACCOUNT_NO= ObjectUtil.checkForNull(rowSplitter.next());
		String LIVE_FINAL_INDICATOR= ObjectUtil.checkForNull(rowSplitter.next());
		String SOURCE_BUSINESS= ObjectUtil.checkForNull(rowSplitter.next());
		String FIBER_INDICATOR= ObjectUtil.checkForNull(rowSplitter.next());
		String KEYING_FLAG= ObjectUtil.checkForNull(rowSplitter.next());
		String EFX_SVC_CNX_ID= ObjectUtil.checkForNull(rowSplitter.next());
		String EFX_SVC_HHLD_ID= ObjectUtil.checkForNull(rowSplitter.next());
		String EFX_SVC_ADDRE_ID= ObjectUtil.checkForNull(rowSplitter.next());
		String EFX_CNX_MODIFY_DT= ObjectUtil.checkForNull(rowSplitter.next());
		
		String BTN_HMAC= barricader.hashIgnoreEmpty(BTN);
		String BILL_STREET_NO_HMAC= barricader.hashIgnoreEmpty(BILL_STREET_NO);
		String BILL_STREET_NAME_HMAC= barricader.hashIgnoreEmpty(BILL_STREET_NAME);
		String BILL_CITY_HMAC= barricader.hashIgnoreEmpty(BILL_CITY);
		String BILL_STATE_HMAC= barricader.hashIgnoreEmpty(BILL_STATE);
		String BILL_ZIP_HMAC= barricader.hashIgnoreEmpty(BILL_ZIP);
		String SVC_STREET_NO_HMAC= barricader.hashIgnoreEmpty(SVC_STREET_NO);
		String SVC_STREET_NAME_HMAC= barricader.hashIgnoreEmpty(SVC_STREET_NAME);
		String SVC_CITY_HMAC= barricader.hashIgnoreEmpty(SVC_CITY);
		String SVC_STATE_HMAC= barricader.hashIgnoreEmpty(SVC_STATE);
		String SVC_ZIP_HMAC= barricader.hashIgnoreEmpty(SVC_ZIP);
		String FIRST_NAME_HMAC= barricader.hashIgnoreEmpty(FIRST_NAME);
		String MIDDLE_NAME_HMAC= barricader.hashIgnoreEmpty(MIDDLE_NAME);
		String LAST_NAME_HMAC= barricader.hashIgnoreEmpty(LAST_NAME);
		String BUSINESS_HMAC_NAME= barricader.hashIgnoreEmpty(BUSINESS_NAME);
		String SSN_TAXID_HMAC= barricader.hashIgnoreEmpty(SSN_TAXID);
		String ACCOUNT_NO_HMAC= barricader.hashIgnoreEmpty(ACCOUNT_NO);
		
		 
		 BTN= barricader.encryptIgnoreEmpty(BTN);
		 BILL_STREET_NO= barricader.encryptIgnoreEmpty(BILL_STREET_NO);
		 BILL_STREET_NAME= barricader.encryptIgnoreEmpty(BILL_STREET_NAME);
		 BILL_CITY= barricader.encryptIgnoreEmpty(BILL_CITY);
		 BILL_STATE= barricader.encryptIgnoreEmpty(BILL_STATE);
		 BILL_ZIP= barricader.encryptIgnoreEmpty(BILL_ZIP);
		 SVC_STREET_NO= barricader.encryptIgnoreEmpty(SVC_STREET_NO);
		 SVC_STREET_NAME= barricader.encryptIgnoreEmpty(SVC_STREET_NAME);
		 SVC_CITY= barricader.encryptIgnoreEmpty(SVC_CITY);
		 SVC_STATE= barricader.encryptIgnoreEmpty(SVC_STATE);
		 SVC_ZIP= barricader.encryptIgnoreEmpty(SVC_ZIP);
		 FIRST_NAME= barricader.encryptIgnoreEmpty(FIRST_NAME);
		 MIDDLE_NAME= barricader.encryptIgnoreEmpty(MIDDLE_NAME);
		 LAST_NAME= barricader.encryptIgnoreEmpty(LAST_NAME);
		 BUSINESS_NAME= barricader.encryptIgnoreEmpty(BUSINESS_NAME);
		 SSN_TAXID= barricader.encryptIgnoreEmpty(SSN_TAXID);
		 ACCOUNT_NO= barricader.encryptIgnoreEmpty(ACCOUNT_NO);
		 
			rowBuilder.addValue(DATASHARE_ID);
			rowBuilder.addValue(LINE_OF_BUSINESS);
			rowBuilder.addValue(BTN);
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
			rowBuilder.addValue(FIRST_NAME);
			rowBuilder.addValue(MIDDLE_NAME);
			rowBuilder.addValue(LAST_NAME);
			rowBuilder.addValue(BUSINESS_NAME);
			rowBuilder.addValue(SSN_TAXID);
			rowBuilder.addValue(ACCOUNT_NO);
			rowBuilder.addValue(LIVE_FINAL_INDICATOR);
			rowBuilder.addValue(SOURCE_BUSINESS);
			rowBuilder.addValue(FIBER_INDICATOR);
			rowBuilder.addValue(KEYING_FLAG);
			rowBuilder.addValue(EFX_SVC_CNX_ID);
			rowBuilder.addValue(EFX_SVC_HHLD_ID);
			rowBuilder.addValue(EFX_SVC_ADDRE_ID);
			rowBuilder.addValue(EFX_CNX_MODIFY_DT);
			rowBuilder.addValue(BTN_HMAC);
			rowBuilder.addValue(BILL_STREET_NO_HMAC);
			rowBuilder.addValue(BILL_STREET_NAME_HMAC);
			rowBuilder.addValue(BILL_CITY_HMAC);
			rowBuilder.addValue(BILL_STATE_HMAC);
			rowBuilder.addValue(BILL_ZIP_HMAC);
			rowBuilder.addValue(SVC_STREET_NO_HMAC);
			rowBuilder.addValue(SVC_STREET_NAME_HMAC);
			rowBuilder.addValue(SVC_CITY_HMAC);
			rowBuilder.addValue(SVC_STATE_HMAC);
			rowBuilder.addValue(SVC_ZIP_HMAC);
			rowBuilder.addValue(FIRST_NAME_HMAC);
			rowBuilder.addValue(MIDDLE_NAME_HMAC);
			rowBuilder.addValue(LAST_NAME_HMAC);
			rowBuilder.addValue(BUSINESS_HMAC_NAME);
			rowBuilder.addValue(SSN_TAXID_HMAC);
			rowBuilder.addValue(ACCOUNT_NO_HMAC);
		return rowBuilder.build();
	}

	private void validate(Row row) throws InvalidFieldException {

		if (null == row.getString("DATASHARE_ID")) {

			throw new InvalidFieldException("Field 'DATASHARE_ID' cannot be null or empty.");

		}
	}

}
