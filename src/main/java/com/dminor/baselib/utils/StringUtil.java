package com.dminor.baselib.utils;

public class StringUtil {
	public static int getIntValue(String str){
		int res = 0;
		if(str == null || str.length() == 0){
			return res;
		}
		try{
			res = Integer.valueOf(str);
		}catch(Exception e){
		}
		return res;
	}
}
