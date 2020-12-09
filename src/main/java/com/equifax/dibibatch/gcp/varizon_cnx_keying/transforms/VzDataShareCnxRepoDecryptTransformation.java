package com.equifax.dibibatch.gcp.varizon_cnx_keying.transforms;

import java.io.IOException;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.ReadableDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.ExtractDataShareSsaConInputSchema;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.LoadVztDataShareCnxRepoFromVwVztDataShareSchema;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.exception.InvalidFieldException;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.BarricaderInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.Barricader;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.ObjectUtil;

public class VzDataShareCnxRepoDecryptTransformation extends DoFn<Row, Row> {

	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggerFactory.getLogger(VzDataShareCnxRepoDecryptTransformation.class);

	BarricaderInfo barricaderInfo = null;
	Barricader barricader = null;

	@StartBundle
	public void startBundle() throws Exception {
		barricader = Barricader.create().withInfo(barricaderInfo).andBuild();
	}

	public VzDataShareCnxRepoDecryptTransformation(BarricaderInfo barricaderInfo) {
		this.barricaderInfo = barricaderInfo;
	}

	@ProcessElement
	public void processElement(ProcessContext c) {

		try {

			Row row = buildRowWithSchema(LoadVztDataShareCnxRepoFromVwVztDataShareSchema.LoadVztDataShareCnxRepo(),
					c.element());
			c.output(row);

		} catch (Exception ex) {

			log.error("Error while processing input", ex);
		}
	}

	private Row buildRowWithSchema(Schema schema, Row inputRow) throws IOException {

		Row.Builder rowBuilder = Row.withSchema(schema);
		


		String DATASHRE_ID = (ObjectUtil.checkForNull(inputRow.getString("DATASHARE_ID")));
		String FIRST_NAME = (ObjectUtil.checkForNull(inputRow.getString("FIRST_NAME")));
		String MIDDLE_NAME = (ObjectUtil.checkForNull(inputRow.getString("MIDDLE_NAME")));
		String LAST_NAME = (ObjectUtil.checkForNull(inputRow.getString("LAST_NAME")));
		String ACCOUNT_NO = (ObjectUtil.checkForNull(inputRow.getString("ACCOUNT_NO")));
		//String EFX_UNIQ_ID = (ObjectUtil.checkForNull(inputRow.getString("EFX_UNIQ_ID")));
		//String EFX_NM = (ObjectUtil.checkForNull(FIRST_NAME + " " + LAST_NAME));// EFX_NM
		//String EFX_ADDR = (ObjectUtil.checkForNull(inputRow.getString("EFX_ADDR")));// EFX_ADDR
		String EFX_CNX_ID = (ObjectUtil.checkForNull(inputRow.getString("EFX_CNX_ID")));
		String EFX_HHLD_ID = (ObjectUtil.checkForNull(inputRow.getString("EFX_HHLD_ID")));
		String EFX_ADDRE_ID = (ObjectUtil.checkForNull(inputRow.getString("EFX_ADDR_ID")));
		String EFX_SOURCE_OF_MATCH = (ObjectUtil.checkForNull(inputRow.getString("EFX_SOURCE_OF_MATCH")));
		String EFX_BEST_KEY_SOURCE = (ObjectUtil.checkForNull(inputRow.getString("EFX_BEST_KEY_SOURCE")));
		ReadableDateTime EFX_CNX_MODIFY_DT = (inputRow.getDateTime("EFX_CNX_MODIFY_DT"));
		String EFX_CONF_CD = (ObjectUtil.checkForNull(inputRow.getString("EFX_CONF_CD")));


		//EFX_UNIQ_ID = barricader.decryptIgnoreEmpty(EFX_UNIQ_ID);
		//EFX_NM = barricader.decryptIgnoreEmpty(EFX_NM);
		//EFX_ADDR = barricader.decryptIgnoreEmpty(EFX_ADDR);

		FIRST_NAME = barricader.decryptIgnoreEmpty(FIRST_NAME);
		MIDDLE_NAME = barricader.decryptIgnoreEmpty(MIDDLE_NAME);
		LAST_NAME = barricader.decryptIgnoreEmpty(LAST_NAME);
		ACCOUNT_NO = barricader.decryptIgnoreEmpty(ACCOUNT_NO);
		//EFX_UNIQ_ID = barricader.decryptIgnoreEmpty(EFX_UNIQ_ID);
		
		
		//rowBuilder.addValue(EFX_UNIQ_ID);
		//rowBuilder.addValue(EFX_NM);
		//rowBuilder.addValue(EFX_ADDR);
		rowBuilder.addValue(DATASHRE_ID);
		rowBuilder.addValue(FIRST_NAME);
		rowBuilder.addValue(MIDDLE_NAME);
		rowBuilder.addValue(LAST_NAME);
		rowBuilder.addValue(ACCOUNT_NO);
		rowBuilder.addValue(EFX_CNX_ID);
		rowBuilder.addValue(EFX_HHLD_ID);
		rowBuilder.addValue(EFX_ADDRE_ID);
		rowBuilder.addValue(EFX_SOURCE_OF_MATCH);
		rowBuilder.addValue(EFX_BEST_KEY_SOURCE);
		rowBuilder.addValue(EFX_CNX_MODIFY_DT);
		rowBuilder.addValue(EFX_CONF_CD);

		return rowBuilder.build();
	}

	private void validate(Row row) throws InvalidFieldException {

	}

}
