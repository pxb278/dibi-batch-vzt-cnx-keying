package com.equifax.dibibatch.gcp.varizon_cnx_keying.transforms;

import java.io.IOException;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.ExtractDataShareSsaConInputSchema;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.exception.InvalidFieldException;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.BarricaderInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.Barricader;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.ObjectUtil;

public class VzDataShareCnxRepoTableDecryptTransformation extends DoFn<Row, Row> {

	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggerFactory.getLogger(VzDataShareCnxRepoDecryptTransformation.class);

	BarricaderInfo barricaderInfo = null;
	Barricader barricader = null;

	@StartBundle
	public void startBundle() throws Exception {
		barricader = Barricader.create().withInfo(barricaderInfo).andBuild();
	}

	public VzDataShareCnxRepoTableDecryptTransformation(BarricaderInfo barricaderInfo) {
		this.barricaderInfo = barricaderInfo;
	}

	@ProcessElement
	public void processElement(ProcessContext c) {

		try {

			Row row = buildRowWithSchema(ExtractDataShareSsaConInputSchema.ExtractExtractDataShareSsaConInput(),
					c.element());
			c.output(row);

		} catch (Exception ex) {

			log.error("Error while processing input", ex);
		}
	}

	private Row buildRowWithSchema(Schema schema, Row inputRow) throws IOException {

		Row.Builder rowBuilder = Row.withSchema(schema);

		String DATASHARE_ID = (ObjectUtil.checkForNull(inputRow.getString("DATASHARE_ID")));
		String FIRST_NAME =(ObjectUtil.checkForNull(inputRow.getString("FIRST_NAME")));
		String MIDDLE_NAME =(ObjectUtil.checkForNull(inputRow.getString("MIDDLE_NAME")));
		String LAST_NAME =(ObjectUtil.checkForNull(inputRow.getString("LAST_NAME")));
		String ACCOUNT_NO =(ObjectUtil.checkForNull(inputRow.getString("ACCOUNT_NO")));
		String EFX_ADDR =(ObjectUtil.checkForNull(inputRow.getString("EFX_ADDR")));
		String EFX_CITY =(ObjectUtil.checkForNull(inputRow.getString("EFX_CITY")));
		String EFX_STATE =(ObjectUtil.checkForNull(inputRow.getString("EFX_STATE")));
		String EFX_ZIP =(ObjectUtil.checkForNull(inputRow.getString("EFX_ZIP")));
		String EFX_CNX_ID =(ObjectUtil.checkForNull(inputRow.getString("EFX_CNX_ID")));
		String EFX_HHLD_ID =(ObjectUtil.checkForNull(inputRow.getString("EFX_HHLD_ID")));
		String EFX_ADDR_ID =(ObjectUtil.checkForNull(inputRow.getString("EFX_ADDR_ID")));
		String EFX_SOURCE_OF_MATCH =(ObjectUtil.checkForNull(inputRow.getString("EFX_SOURCE_OF_MATCH")));
		String EFX_BEST_KEY_SOURCE =(ObjectUtil.checkForNull(inputRow.getString("EFX_BEST_KEY_SOURCE")));
		String EFX_CNX_MODIFY_DT =(ObjectUtil.checkForNull(inputRow.getString("EFX_CNX_MODIFY_DT")));
		String EFX_CONF_CD =(ObjectUtil.checkForNull(inputRow.getString("EFX_CONF_CD")));

		 FIRST_NAME =barricader.decryptIgnoreEmpty(FIRST_NAME);
		 MIDDLE_NAME =barricader.decryptIgnoreEmpty(MIDDLE_NAME);
		 LAST_NAME =barricader.decryptIgnoreEmpty(LAST_NAME);
		 ACCOUNT_NO =barricader.decryptIgnoreEmpty(ACCOUNT_NO);
		 EFX_ADDR =barricader.decryptIgnoreEmpty(EFX_ADDR);
		 EFX_CITY =barricader.decryptIgnoreEmpty(EFX_CITY);
		 EFX_STATE =barricader.decryptIgnoreEmpty(EFX_STATE);
		 EFX_ZIP =barricader.decryptIgnoreEmpty(EFX_ZIP);
		
		rowBuilder.addValue(DATASHARE_ID);
		rowBuilder.addValue(FIRST_NAME);
		rowBuilder.addValue(MIDDLE_NAME);
		rowBuilder.addValue(LAST_NAME);
		rowBuilder.addValue(ACCOUNT_NO);
		rowBuilder.addValue(EFX_ADDR);
		rowBuilder.addValue(EFX_CITY);
		rowBuilder.addValue(EFX_STATE);
		rowBuilder.addValue(EFX_ZIP);
		rowBuilder.addValue(EFX_CNX_ID);
		rowBuilder.addValue(EFX_HHLD_ID);
		rowBuilder.addValue(EFX_ADDR_ID);
		rowBuilder.addValue(EFX_SOURCE_OF_MATCH);
		rowBuilder.addValue(EFX_BEST_KEY_SOURCE);
		rowBuilder.addValue(EFX_CNX_MODIFY_DT);
		rowBuilder.addValue(EFX_CONF_CD);

		return rowBuilder.build();
	}

	private void validate(Row row) throws InvalidFieldException {

	}

}