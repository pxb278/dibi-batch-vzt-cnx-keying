package com.equifax.dibibatch.gcp.varizon_cnx_keying.transforms;

import java.text.ParseException;
import java.util.Iterator;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.LoadFromInactiveOvrerrideRecordsFileSchema;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.exception.InvalidFieldException;
import com.google.common.base.Splitter;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class LoadFromInactiveOvrerrideRecordsTransform extends DoFn<String, Row> {

	private static final long serialVersionUID = -6771720184879823368L;

	private static final Logger log = LoggerFactory.getLogger(ExtractFromStDsOutToLoadVztDataShareTransform.class);
	private static final String HEADER = "DATASHRE_ID|OLD_BEST_CNX_ID|EFX_CNX_ID|EFX_HHLD_ID|EFX_ADDR_ID|EFX_OVERRIDE_CNX_ID|EFX_OVERRIDE_HHLD_ID";

	TupleTag validRowsTag;
	TupleTag invalidRowsTag;
	ValueProvider<String> currentTimestamp;

	public LoadFromInactiveOvrerrideRecordsTransform(TupleTag validRowsTag, TupleTag invalidRowsTag, ValueProvider<String> currentTimestamp) {

		this.validRowsTag = validRowsTag;
		this.invalidRowsTag = invalidRowsTag;
		this.currentTimestamp = currentTimestamp;
	}

	@ProcessElement
	public void processElement(ProcessContext c) {

		try {

			Row row = buildRowWithSchema(
					LoadFromInactiveOvrerrideRecordsFileSchema.extractInactiveOvrerrideRecordsFile(), c.element());

			validate(row);

			c.output(this.validRowsTag, row);

		} catch (Exception ex) {

			log.error("Exception occurred for: " + c.element(), ex);

			c.output(this.invalidRowsTag, c.element() + "|" + ex.getMessage());
		}
	}

	private Row buildRowWithSchema(Schema schema, String input) throws ParseException {
		
		if(input.trim().equals(HEADER)) {
			return null;
		}

		Row.Builder rowBuilder = Row.withSchema(schema);

		Iterator<String> rowSplitter = Splitter.on("|").trimResults().split(input).iterator();

		rowBuilder.addValue(rowSplitter.next());
		rowBuilder.addValue(rowSplitter.next());
		rowBuilder.addValue(rowSplitter.next());
		rowBuilder.addValue(rowSplitter.next());
		rowBuilder.addValue(rowSplitter.next());
		rowBuilder.addValue(rowSplitter.next());
		rowBuilder.addValue(rowSplitter.next());
		//rowBuilder.addValue(CalendarUtil.convertStringToTimestamp(currentTimestamp.get(), "yyyy-MM-dd'T'HH:mm:ss"));
		rowBuilder.addValue(currentTimestamp.get());


		return rowBuilder.build();
	}

	private void validate(Row row) throws InvalidFieldException {

	}
}
