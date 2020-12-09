package com.equifax.dibibatch.gcp.varizon_cnx_keying.transforms;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.DateTime;

public class HeaderFn<T> extends PTransform<PCollection<T>, PCollectionView<String>> {

	private static final long serialVersionUID = 1L;

	private ValueProvider<String> fileName;

	public HeaderFn(ValueProvider<String> fileName) {
		super();
		this.fileName = fileName;
	}

	@Override
	public PCollectionView<String> expand(PCollection<T> input) {

		return input
				.apply("count", Count.<T>globally())
				.apply(ParDo.of(new DoFn<Long, String>() {

					private static final long serialVersionUID = 1L;
		
					@ProcessElement
					public void process(ProcessContext c) {
		
						Long count = c.element();
						
						Long countIncludingHeaderAndTrailer = count + 2;
						
						DateTime datetime = new DateTime();
;
						
						String date = datetime.getYear() + "/" + datetime.getMonthOfYear() + "/" + datetime.getDayOfMonth();
						String time = datetime.getHourOfDay() + ":" + datetime.getMinuteOfHour() + ":" + datetime.getSecondOfMinute();

						
						StringBuilder sb = new StringBuilder();
						String header = sb.append("HR|")
						.append(fileName)
						.append("|DATASHARE EFXID RESPONSE FILE|")
						.append(countIncludingHeaderAndTrailer)
						.append("|")
						.append(date)
						.append("|")
						.append(time)
						.toString();
						
						c.output(header);
					}
				}))
				.apply(View.asSingleton());
	}

}
