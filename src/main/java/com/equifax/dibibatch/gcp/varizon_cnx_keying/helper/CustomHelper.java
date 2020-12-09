package com.equifax.dibibatch.gcp.varizon_cnx_keying.helper;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.ObjectUtil;

public class CustomHelper {


	public static void setFromDateTime(Row row, String fieldName, PreparedStatement preparedStatement, int index)
			throws SQLException {

		if (null != row.getDateTime(fieldName)) {

			preparedStatement.setTimestamp(index, ObjectUtil.convertToTimestamp(row.getDateTime(fieldName)));

		} else {

			preparedStatement.setObject(index, null);
		}

	}

	public static void setFromFloat(Row row, String fieldName, PreparedStatement preparedStatement, int index)
			throws SQLException {

		if (null != row.getFloat(fieldName)) {

			preparedStatement.setFloat(index, row.getFloat(fieldName));

		} else {

			preparedStatement.setObject(index, null);
		}

	}

	public static void setToMilis(Row row, String fieldName, PreparedStatement preparedStatement, int index)
			throws SQLException {

		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		long time = timestamp.getTime() + index;
		preparedStatement.setLong(index, time);
	}

	public static String subStringResponse(String originalString, int startingPostion, int numberOfCharacters,
			boolean doTrim) {

		int actualStartingPoint = startingPostion - 1;
		int substringTill = actualStartingPoint + numberOfCharacters;
		if (doTrim) {

			String OutputResponse = (originalString.substring(actualStartingPoint, substringTill)).trim();

			return OutputResponse;
		} else
			return (originalString.substring(actualStartingPoint, substringTill));
	}

	public static DateTime getCurrentDate() {
		DateTime dt = new DateTime();
		return dt;

	}

	public static DateTime subStringDate(String originalString, int startingPostion, int numberOfCharacters,
			boolean doTrim, String format) {
		DateTime out = null;
		int actualStartingPoint = startingPostion - 1;
		int substringTill = actualStartingPoint + numberOfCharacters;
		if (doTrim) {

			String outputResponse = (originalString.substring(actualStartingPoint, substringTill)).trim();

			if (outputResponse.equals("00000000") || outputResponse.equals("")) {
				out = null;
			} else {
				DateTimeFormatter formatter = DateTimeFormat.forPattern(format);
				long millis = formatter.parseMillis(outputResponse);
				out = new DateTime(millis);
			}
		}
		return out;
	}

	public static String getDateStringFromFileName(String originalString, int startingPostion, int numberOfCharacters) {
		
		int actualStartingPoint = startingPostion - 1;
		int substringTill = actualStartingPoint + numberOfCharacters;

		String outputResponse = (originalString.substring(actualStartingPoint, substringTill)).trim();

		return outputResponse;
	}

	public static DateTime getDateFromFileName(String originalString, int startingPostion, int numberOfCharacters,String format) {
		
		int actualStartingPoint = startingPostion - 1;
		int substringTill = actualStartingPoint + numberOfCharacters;

		String dateString = (originalString.substring(actualStartingPoint, substringTill)).trim();
		return convertStringToTimestamp(dateString,format);
	}
	
	public static DateTime convertStringToTimestamp(String input, String format) {
		DateTime result = null;
		try {
			if (null != input && input.trim().length() > 0) {
				SimpleDateFormat dateFormat = new SimpleDateFormat(format);
				Date parsedDate = dateFormat.parse(input);
				result = new DateTime(parsedDate.getTime());
			}
		} catch (Exception e) {
			result = null;
		}
		return result;
	}
	
	/**
	 * Replace multiple occurrence of space with single space
	 * @return
	 */
	public static String replaceSpace(String string) {
	      Scanner sc = new Scanner(string);
	      String input = sc.nextLine();
	      String regex = "\\s+";
	      //Compiling the regular expression
	      Pattern pattern = Pattern.compile(regex);
	      //Retrieving the matcher object
	      Matcher matcher = pattern.matcher(input);
	      //Replacing all space characters with single space
	      String result = matcher.replaceAll(" ");
	      return result;
		
	}
	
	
	public static String coalesce (String str1, String str2) {
		
		if (str1 != null) {
			return str1;
		} else {
			return str2;
		}
	}

	
	public static void main(String[] args) {
		//String filename = "VZT_DS_PROD.EFXID.KREQ.0000392.20180712112233";
		//String filename = "VZT_DS_PROD.EFXID.KREQ.0000392.20170225112233";
		//System.out.println("Date:"+getDateFromFileName(filename,32,8,"yyyyMMdd"));
		
		String date = getCurrentDate().getYear() + "/" + getCurrentDate().getMonthOfYear() + "/" + getCurrentDate().getDayOfMonth();
		String time = getCurrentDate().getHourOfDay() + ":" + getCurrentDate().getMinuteOfHour() + ":" + getCurrentDate().getSecondOfMinute();
		System.out.println("date-->"+ date);
		System.out.println("time-->"+ time);

		
	}
}
