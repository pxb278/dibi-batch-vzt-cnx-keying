package com.equifax.dibibatch.gcp.varizon_cnx_keying.utils;

import java.math.BigDecimal;

public class ObjectUtils {
	
	public static Long nullToZero(Long i) {
		return (i == null) ? 0 : i;
	}

	public static BigDecimal nullToZero(BigDecimal i) {
		Integer z = 0;
		return (i != null) ? i : BigDecimal.ZERO;
	}


	public static String transformationForEfxBillCnxErrorCode(String input) {
		if (null == input) {
			return null;
		} else if (input.equalsIgnoreCase("ERRJCT")) {
			return "ERNAME";
		} else
			return input.trim();
	}

	public static String transformationForEfxSvcCnxErrorCode(String input) {
		if (null == input) {
			return null;
		} else if (input.equalsIgnoreCase("ERRJCT")) {
			return "ERNAME";
		} else
			return input.trim();
	}
	
	public static String transformationForEfxErrorCode(String input) {
		if (null == input) {
			return null;
		} else if (input.equalsIgnoreCase("ERNAME")) {
			return "102";
		}
		else if (input.equalsIgnoreCase("ERADDR")) {
			return "103";
		}else
			return "104";
	}
}
