package com.equifax.dibibatch.gcp.varizon_cnx_keying.helper;

import java.sql.PreparedStatement;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline.LoadVztDataShareTablePipeLine;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.CalendarUtil;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcCommons;

public class VztDataShareTableHelper {

	private static final Logger log = LoggerFactory.getLogger(VztDataShareTableHelper.class);

	public static void insertVztDataShareRowWithKeyingFlagF(Row row, PreparedStatement preparedStatement,
			ValueProvider<String> currentTimestamp, ValueProvider<Integer> batchId, ValueProvider<String> processName)
			throws Exception {
		if (row.getString("KEYING_FLAG").equalsIgnoreCase("F")) {

			try {

				JdbcCommons.setFromString(row, "DATASHARE_ID", preparedStatement, 1);
				JdbcCommons.setFromString(row, "LINE_OF_BUSINESS", preparedStatement, 2);
				JdbcCommons.setFromString(row, "BTN", preparedStatement, 3);
				JdbcCommons.setFromString(row, "BILL_STREET_NO", preparedStatement, 4);
				JdbcCommons.setFromString(row, "BILL_STREET_NAME", preparedStatement, 5);
				JdbcCommons.setFromString(row, "BILL_CITY", preparedStatement, 6);
				JdbcCommons.setFromString(row, "BILL_STATE", preparedStatement, 7);
				JdbcCommons.setFromString(row, "BILL_ZIP", preparedStatement, 8);
				JdbcCommons.setFromString(row, "SVC_STREET_NO", preparedStatement, 9);
				JdbcCommons.setFromString(row, "SVC_STREET_NAME", preparedStatement, 10);
				JdbcCommons.setFromString(row, "SVC_CITY", preparedStatement, 11);
				JdbcCommons.setFromString(row, "SVC_STATE", preparedStatement, 12);
				JdbcCommons.setFromString(row, "SVC_ZIP", preparedStatement, 13);
				JdbcCommons.setFromString(row, "FIRST_NAME", preparedStatement, 14);
				JdbcCommons.setFromString(row, "MIDDLE_NAME", preparedStatement, 15);
				JdbcCommons.setFromString(row, "LAST_NAME", preparedStatement, 16);
				JdbcCommons.setFromString(row, "BUSINESS_NAME", preparedStatement, 17);
				JdbcCommons.setFromString(row, "SSN_TAXID", preparedStatement, 18);
				JdbcCommons.setFromString(row, "ACCOUNT_NO", preparedStatement, 19);
				JdbcCommons.setFromString(row, "LIVE_FINAL_INDICATOR", preparedStatement, 20);
				JdbcCommons.setFromString(row, "SOURCE_BUSINESS", preparedStatement, 21);
				JdbcCommons.setFromString(row, "FIBER_INDICATOR", preparedStatement, 22);
				JdbcCommons.setFromString(row, "KEYING_FLAG", preparedStatement, 23);
				JdbcCommons.setFromString(row, "EFX_SVC_CNX_ID", preparedStatement, 24);
				JdbcCommons.setFromString(row, "EFX_SVC_HHLD_ID", preparedStatement, 25);
				JdbcCommons.setFromString(row, "EFX_SVC_ADDR_ID", preparedStatement, 26);
				preparedStatement.setTimestamp(27,
						CalendarUtil.convertStringToTimestamp(row.getString("EFX_CNX_MODIFY_DT"), "MM-dd-yyyy"));

				preparedStatement.setString(28, "S");
				preparedStatement.setString(29, processName.get());

				preparedStatement.setTimestamp(30,
						CalendarUtil.convertStringToTimestamp(currentTimestamp.get(), "MM-dd-yyyy"));

				preparedStatement.setLong(31, batchId.get());
				preparedStatement.setTimestamp(32,
						CalendarUtil.convertStringToTimestamp(currentTimestamp.get(), "MM-dd-yyyy"));
				preparedStatement.setLong(33, batchId.get());
				JdbcCommons.setFromString(row, "BTN_HMAC", preparedStatement, 34);
				JdbcCommons.setFromString(row, "BILL_STREET_NO_HMAC", preparedStatement, 35);
				JdbcCommons.setFromString(row, "BILL_STREET_NAME_HMAC", preparedStatement, 36);
				JdbcCommons.setFromString(row, "BILL_CITY_HMAC", preparedStatement, 37);
				JdbcCommons.setFromString(row, "BILL_STATE_HMAC", preparedStatement, 38);
				JdbcCommons.setFromString(row, "BILL_ZIP_HMAC", preparedStatement, 39);
				JdbcCommons.setFromString(row, "SVC_STREET_NO_HMAC", preparedStatement, 40);
				JdbcCommons.setFromString(row, "SVC_STREET_NAME_HMAC", preparedStatement, 41);
				JdbcCommons.setFromString(row, "SVC_CITY_HMAC", preparedStatement, 42);
				JdbcCommons.setFromString(row, "SVC_STATE_HMAC", preparedStatement, 43);
				JdbcCommons.setFromString(row, "SVC_ZIP_HMAC", preparedStatement, 44);
				JdbcCommons.setFromString(row, "FIRST_NAME_HMAC", preparedStatement, 45);
				JdbcCommons.setFromString(row, "MIDDLE_NAME_HMAC", preparedStatement, 46);
				JdbcCommons.setFromString(row, "LAST_NAME_HMAC", preparedStatement, 47);
				JdbcCommons.setFromString(row, "BUSINESS_NAME_HMAC", preparedStatement, 48);
				JdbcCommons.setFromString(row, "SSN_TAXID_HMAC", preparedStatement, 49);
				JdbcCommons.setFromString(row, "ACCOUNT_NO_HMAC", preparedStatement, 50);

			} catch (Exception ex) {

				log.error("Exception occurred: " + ex.getMessage(), ex);

				throw ex;
			}
		}

	}

	public static void updateVztDataShareRowWithKeyingFlagF(Row row, PreparedStatement preparedStatement,
			ValueProvider<String> currentTimestamp, ValueProvider<Integer> batchId, ValueProvider<String> processName)
			throws Exception {
		if (row.getString("KEYING_FLAG").equalsIgnoreCase("F")) {

			try {

				JdbcCommons.setFromString(row, "LINE_OF_BUSINESS", preparedStatement, 1);
				JdbcCommons.setFromString(row, "BTN", preparedStatement, 2);
				JdbcCommons.setFromString(row, "BILL_STREET_NO", preparedStatement, 3);
				JdbcCommons.setFromString(row, "BILL_STREET_NAME", preparedStatement, 4);
				JdbcCommons.setFromString(row, "BILL_CITY", preparedStatement, 5);
				JdbcCommons.setFromString(row, "BILL_STATE", preparedStatement, 6);
				JdbcCommons.setFromString(row, "BILL_ZIP", preparedStatement, 7);
				JdbcCommons.setFromString(row, "SVC_STREET_NO", preparedStatement, 8);
				JdbcCommons.setFromString(row, "SVC_STREET_NAME", preparedStatement, 9);
				JdbcCommons.setFromString(row, "SVC_CITY", preparedStatement, 10);
				JdbcCommons.setFromString(row, "SVC_STATE", preparedStatement, 11);
				JdbcCommons.setFromString(row, "SVC_ZIP", preparedStatement, 12);
				JdbcCommons.setFromString(row, "FIRST_NAME", preparedStatement, 13);
				JdbcCommons.setFromString(row, "MIDDLE_NAME", preparedStatement, 14);
				JdbcCommons.setFromString(row, "LAST_NAME", preparedStatement, 15);
				JdbcCommons.setFromString(row, "BUSINESS_NAME", preparedStatement, 16);
				JdbcCommons.setFromString(row, "SSN_TAXID", preparedStatement, 17);
				JdbcCommons.setFromString(row, "ACCOUNT_NO", preparedStatement, 18);
				JdbcCommons.setFromString(row, "LIVE_FINAL_INDICATOR", preparedStatement, 19);
				JdbcCommons.setFromString(row, "SOURCE_BUSINESS", preparedStatement, 20);
				JdbcCommons.setFromString(row, "FIBER_INDICATOR", preparedStatement, 21);
				JdbcCommons.setFromString(row, "KEYING_FLAG", preparedStatement, 22);
				JdbcCommons.setFromString(row, "EFX_SVC_CNX_ID", preparedStatement, 23);
				JdbcCommons.setFromString(row, "EFX_SVC_HHLD_ID", preparedStatement, 24);
				JdbcCommons.setFromString(row, "EFX_SVC_ADDR_ID", preparedStatement, 25);
				preparedStatement.setTimestamp(26,
						CalendarUtil.convertStringToTimestamp(row.getString("EFX_CNX_MODIFY_DT"), "MM-dd-yyyy"));
				preparedStatement.setString(27, "S");
				preparedStatement.setString(28, processName.get());
				preparedStatement.setTimestamp(29,
						CalendarUtil.convertStringToTimestamp(currentTimestamp.get(), "MM-dd-yyyy"));
				preparedStatement.setLong(30, batchId.get());

				JdbcCommons.setFromString(row, "BTN_HMAC", preparedStatement, 31);
				JdbcCommons.setFromString(row, "BILL_STREET_NO_HMAC", preparedStatement, 32);
				JdbcCommons.setFromString(row, "BILL_STREET_NAME_HMAC", preparedStatement, 33);
				JdbcCommons.setFromString(row, "BILL_CITY_HMAC", preparedStatement, 34);
				JdbcCommons.setFromString(row, "BILL_STATE_HMAC", preparedStatement, 35);
				JdbcCommons.setFromString(row, "BILL_ZIP_HMAC", preparedStatement, 36);
				JdbcCommons.setFromString(row, "SVC_STREET_NO_HMAC", preparedStatement, 37);
				JdbcCommons.setFromString(row, "SVC_STREET_NAME_HMAC", preparedStatement, 38);
				JdbcCommons.setFromString(row, "SVC_CITY_HMAC", preparedStatement, 39);
				JdbcCommons.setFromString(row, "SVC_STATE_HMAC", preparedStatement, 40);
				JdbcCommons.setFromString(row, "SVC_ZIP_HMAC", preparedStatement, 41);
				JdbcCommons.setFromString(row, "FIRST_NAME_HMAC", preparedStatement, 42);
				JdbcCommons.setFromString(row, "MIDDLE_NAME_HMAC", preparedStatement, 43);
				JdbcCommons.setFromString(row, "LAST_NAME_HMAC", preparedStatement, 44);
				JdbcCommons.setFromString(row, "BUSINESS_NAME_HMAC", preparedStatement, 45);
				JdbcCommons.setFromString(row, "SSN_TAXID_HMAC", preparedStatement, 46);
				JdbcCommons.setFromString(row, "ACCOUNT_NO_HMAC", preparedStatement, 47);
				JdbcCommons.setFromString(row, "DATASHARE_ID", preparedStatement, 48);

			} catch (Exception ex) {

				log.error("Exception occurred: " + ex.getMessage(), ex);

				throw ex;
			}
		}
	}

	public static void updateVztDataShareRowWithKeyingFlagT(Row row, PreparedStatement preparedStatement,
			ValueProvider<String> currentTimestamp, ValueProvider<Integer> batchId, ValueProvider<String> processName)
			throws Exception {
		if (row.getString("KEYING_FLAG").equalsIgnoreCase("T")) {
			try {

				JdbcCommons.setFromString(row, "LINE_OF_BUSINESS", preparedStatement, 1);
				JdbcCommons.setFromString(row, "BTN", preparedStatement, 2);
				JdbcCommons.setFromString(row, "BILL_STREET_NO", preparedStatement, 3);
				JdbcCommons.setFromString(row, "BILL_STREET_NAME", preparedStatement, 4);
				JdbcCommons.setFromString(row, "BILL_CITY", preparedStatement, 5);
				JdbcCommons.setFromString(row, "BILL_STATE", preparedStatement, 6);
				JdbcCommons.setFromString(row, "BILL_ZIP", preparedStatement, 7);
				JdbcCommons.setFromString(row, "SVC_STREET_NO", preparedStatement, 8);
				JdbcCommons.setFromString(row, "SVC_STREET_NAME", preparedStatement, 9);
				JdbcCommons.setFromString(row, "SVC_CITY", preparedStatement, 10);
				JdbcCommons.setFromString(row, "SVC_STATE", preparedStatement, 11);
				JdbcCommons.setFromString(row, "SVC_ZIP", preparedStatement, 12);
				JdbcCommons.setFromString(row, "FIRST_NAME", preparedStatement, 13);
				JdbcCommons.setFromString(row, "MIDDLE_NAME", preparedStatement, 14);
				JdbcCommons.setFromString(row, "LAST_NAME", preparedStatement, 15);
				JdbcCommons.setFromString(row, "BUSINESS_NAME", preparedStatement, 16);
				JdbcCommons.setFromString(row, "SSN_TAXID", preparedStatement, 17);
				JdbcCommons.setFromString(row, "ACCOUNT_NO", preparedStatement, 18);
				JdbcCommons.setFromString(row, "LIVE_FINAL_INDICATOR", preparedStatement, 19);
				JdbcCommons.setFromString(row, "SOURCE_BUSINESS", preparedStatement, 20);
				JdbcCommons.setFromString(row, "FIBER_INDICATOR", preparedStatement, 21);
				JdbcCommons.setFromString(row, "KEYING_FLAG", preparedStatement, 22);
				preparedStatement.setString(23, processName.get());
				preparedStatement.setTimestamp(24,
						CalendarUtil.convertStringToTimestamp(currentTimestamp.get(), "MM-dd-yyyy"));
				preparedStatement.setLong(25, batchId.get());

				JdbcCommons.setFromString(row, "BTN_HMAC", preparedStatement, 26);
				JdbcCommons.setFromString(row, "BILL_STREET_NO_HMAC", preparedStatement, 27);
				JdbcCommons.setFromString(row, "BILL_STREET_NAME_HMAC", preparedStatement, 28);
				JdbcCommons.setFromString(row, "BILL_CITY_HMAC", preparedStatement, 29);
				JdbcCommons.setFromString(row, "BILL_STATE_HMAC", preparedStatement, 30);
				JdbcCommons.setFromString(row, "BILL_ZIP_HMAC", preparedStatement, 31);
				JdbcCommons.setFromString(row, "SVC_STREET_NO_HMAC", preparedStatement, 32);
				JdbcCommons.setFromString(row, "SVC_STREET_NAME_HMAC", preparedStatement, 33);
				JdbcCommons.setFromString(row, "SVC_CITY_HMAC", preparedStatement, 34);
				JdbcCommons.setFromString(row, "SVC_STATE_HMAC", preparedStatement, 35);
				JdbcCommons.setFromString(row, "SVC_ZIP_HMAC", preparedStatement, 36);
				JdbcCommons.setFromString(row, "FIRST_NAME_HMAC", preparedStatement, 37);
				JdbcCommons.setFromString(row, "MIDDLE_NAME_HMAC", preparedStatement, 38);
				JdbcCommons.setFromString(row, "LAST_NAME_HMAC", preparedStatement, 39);
				JdbcCommons.setFromString(row, "BUSINESS_NAME_HMAC", preparedStatement, 40);
				JdbcCommons.setFromString(row, "SSN_TAXID_HMAC", preparedStatement, 41);
				JdbcCommons.setFromString(row, "ACCOUNT_NO_HMAC", preparedStatement, 42);
				JdbcCommons.setFromString(row, "DATASHARE_ID", preparedStatement, 43);
			} catch (Exception ex) {

				log.error("Exception occurred: " + ex.getMessage(), ex);

				throw ex;
			}
		}
	}

	public static void insertVztDataShareRowWithKeyingFlagT(Row row, PreparedStatement preparedStatement,
			ValueProvider<String> currentTimestamp, ValueProvider<Integer> batchId, ValueProvider<String> processName)
			throws Exception {

		if (row.getString("KEYING_FLAG").equalsIgnoreCase("T")) {
			try {
				JdbcCommons.setFromString(row, "DATASHARE_ID", preparedStatement, 1);
				JdbcCommons.setFromString(row, "LINE_OF_BUSINESS", preparedStatement, 2);
				JdbcCommons.setFromString(row, "BTN", preparedStatement, 3);
				JdbcCommons.setFromString(row, "BILL_STREET_NO", preparedStatement, 4);
				JdbcCommons.setFromString(row, "BILL_STREET_NAME", preparedStatement, 5);
				JdbcCommons.setFromString(row, "BILL_CITY", preparedStatement, 6);
				JdbcCommons.setFromString(row, "BILL_STATE", preparedStatement, 7);
				JdbcCommons.setFromString(row, "BILL_ZIP", preparedStatement, 8);
				JdbcCommons.setFromString(row, "SVC_STREET_NO", preparedStatement, 9);
				JdbcCommons.setFromString(row, "SVC_STREET_NAME", preparedStatement, 10);
				JdbcCommons.setFromString(row, "SVC_CITY", preparedStatement, 11);
				JdbcCommons.setFromString(row, "SVC_STATE", preparedStatement, 12);
				JdbcCommons.setFromString(row, "SVC_ZIP", preparedStatement, 13);
				JdbcCommons.setFromString(row, "FIRST_NAME", preparedStatement, 14);
				JdbcCommons.setFromString(row, "MIDDLE_NAME", preparedStatement, 15);
				JdbcCommons.setFromString(row, "LAST_NAME", preparedStatement, 16);
				JdbcCommons.setFromString(row, "BUSINESS_NAME", preparedStatement, 17);
				JdbcCommons.setFromString(row, "SSN_TAXID", preparedStatement, 18);
				JdbcCommons.setFromString(row, "ACCOUNT_NO", preparedStatement, 19);
				JdbcCommons.setFromString(row, "LIVE_FINAL_INDICATOR", preparedStatement, 20);
				JdbcCommons.setFromString(row, "SOURCE_BUSINESS", preparedStatement, 21);
				JdbcCommons.setFromString(row, "FIBER_INDICATOR", preparedStatement, 22);
				JdbcCommons.setFromString(row, "KEYING_FLAG", preparedStatement, 23);
				preparedStatement.setString(24, processName.get());
				preparedStatement.setTimestamp(25,
						CalendarUtil.convertStringToTimestamp(currentTimestamp.get(), "MM-dd-yyyy"));
				preparedStatement.setLong(26, batchId.get());
				preparedStatement.setTimestamp(27,
						CalendarUtil.convertStringToTimestamp(currentTimestamp.get(), "MM-dd-yyyy"));
				preparedStatement.setLong(28, batchId.get());
				JdbcCommons.setFromString(row, "BTN_HMAC", preparedStatement, 29);
				JdbcCommons.setFromString(row, "BILL_STREET_NO_HMAC", preparedStatement, 30);
				JdbcCommons.setFromString(row, "BILL_STREET_NAME_HMAC", preparedStatement, 31);
				JdbcCommons.setFromString(row, "BILL_CITY_HMAC", preparedStatement, 32);
				JdbcCommons.setFromString(row, "BILL_STATE_HMAC", preparedStatement, 33);
				JdbcCommons.setFromString(row, "BILL_ZIP_HMAC", preparedStatement, 34);
				JdbcCommons.setFromString(row, "SVC_STREET_NO_HMAC", preparedStatement, 35);
				JdbcCommons.setFromString(row, "SVC_STREET_NAME_HMAC", preparedStatement, 36);
				JdbcCommons.setFromString(row, "SVC_CITY_HMAC", preparedStatement, 37);
				JdbcCommons.setFromString(row, "SVC_STATE_HMAC", preparedStatement, 38);
				JdbcCommons.setFromString(row, "SVC_ZIP_HMAC", preparedStatement, 39);
				JdbcCommons.setFromString(row, "FIRST_NAME_HMAC", preparedStatement, 40);
				JdbcCommons.setFromString(row, "MIDDLE_NAME_HMAC", preparedStatement, 41);
				JdbcCommons.setFromString(row, "LAST_NAME_HMAC", preparedStatement, 42);
				JdbcCommons.setFromString(row, "BUSINESS_NAME_HMAC", preparedStatement, 43);
				JdbcCommons.setFromString(row, "SSN_TAXID_HMAC", preparedStatement, 44);
				JdbcCommons.setFromString(row, "ACCOUNT_NO_HMAC", preparedStatement, 45);
			} catch (Exception ex) {

				log.error("Exception occurred: " + ex.getMessage(), ex);

				throw ex;
			}
		}
	}

}
