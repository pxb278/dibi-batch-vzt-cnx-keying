package com.equifax.dibibatch.gcp.varizon_cnx_keying.transforms;

import java.text.ParseException;
import java.util.Iterator;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.ExtractDICNX0000002EfxVzwFileSchema;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.exception.InvalidFieldException;
import com.google.common.base.Splitter;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class ExtractDICNX0000002EfxVzwFileTransform extends DoFn<String, Row> {

	private static final long serialVersionUID = -6771720184879823368L;

	private static final Logger log = LoggerFactory.getLogger(ExtractDICNX0000002EfxVzwFileTransform.class);

	TupleTag validRowsTag;
	TupleTag invalidRowsTag;

	public ExtractDICNX0000002EfxVzwFileTransform(TupleTag validRowsTag, TupleTag invalidRowsTag) {

		this.validRowsTag = validRowsTag;
		this.invalidRowsTag = invalidRowsTag;
	}

	@ProcessElement
	public void processElement(ProcessContext c) {

		try {

			Row row = buildRowWithSchema(ExtractDICNX0000002EfxVzwFileSchema.extractDICNX0000002EfxVzwFile(),
					c.element());

			validate(row);

			c.output(this.validRowsTag, row);

		} catch (Exception ex) {

			log.error("Exception occurred for: " + c.element(), ex);

			c.output(this.invalidRowsTag, c.element() + "|" + ex.getMessage());
		}
	}

	private Row buildRowWithSchema(Schema schema, String input) throws ParseException {

		Row.Builder rowBuilder = Row.withSchema(schema);

		Iterator<String> rowSplitter = Splitter.on("|").trimResults().split(input).iterator();
		rowBuilder.addValue(rowSplitter.next());
		rowBuilder.addValue(rowSplitter.next());
		rowBuilder.addValue(rowSplitter.next());
		rowBuilder.addValue(rowSplitter.next());
		rowBuilder.addValue(rowSplitter.next());
		rowBuilder.addValue(rowSplitter.next());
		rowBuilder.addValue(rowSplitter.next());
		rowBuilder.addValue(rowSplitter.next());
		rowBuilder.addValue(rowSplitter.next());
		rowBuilder.addValue(rowSplitter.next());
		rowBuilder.addValue(rowSplitter.next());
		rowBuilder.addValue(rowSplitter.next());
		rowBuilder.addValue(rowSplitter.next());
		

		return rowBuilder.build();
	}

	private void validate(Row row) throws InvalidFieldException {

	}
}
