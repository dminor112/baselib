package com.dminor.baselib.utils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonUtil {
	
	public static Pattern jsonPattern = Pattern.compile("(\\{\\{*\\[*\".*\\]*\\}*\\})|(\\[\\{*\\[*\".*\\]*\\}*\\])");
	private static Logger log = LoggerFactory.getLogger("cmsinfo");
	
	
	public static <T> List<T> StringToObjectList(String str,TypeReference<?> type){
		try {
			if(str==null || str.isEmpty() || type==null){
				//TODO
				return null;
			}
			ObjectMapper objectMapper = new ObjectMapper();
			return objectMapper.readValue(str,type);
		} catch (JsonParseException e) {
			log.error("JsonParseException,{}", str, e);
		} catch (JsonMappingException e) {
			log.error("JsonMappingException,{}", str, e);
		} catch (IOException e) {
			log.error("IOException,{}", str, e);
		}
		return null;
	}
	
	public static <T> T StringToObject(String str,TypeReference<T> type){
		try {
			if(str==null || str.isEmpty() || type==null){
				//TODO
				return null;
			}
			ObjectMapper objectMapper = new ObjectMapper();
			return objectMapper.readValue(str,type);
		} catch (JsonParseException e) {
			log.error("JsonParseException,{}", str, e);
		} catch (JsonMappingException e) {
			log.error("JsonMappingException,{}", str, e);
		} catch (IOException e) {
			log.error("IOException,{}", str, e);
		}
		return null;
	}
	
	public static String ObjectToString(Object obj){
		try {
			if(obj==null){
				//TODO
				return null;
			}
			ObjectMapper objectMapper = new ObjectMapper();
			return objectMapper.writeValueAsString(obj);
		} catch (JsonProcessingException e) {
			log.error("JsonProcessingException", e);
			return null;
		}
		
	}
	
	public static JsonNode StringToJsonNode(String json){
		try {
			ObjectMapper objectMapper = new ObjectMapper();
			JsonNode jsonNode = objectMapper.readTree(json);
			return jsonNode;
		} catch (JsonParseException e) {
			log.error("JsonParseException,{}", json, e);
		} catch (JsonMappingException e) {
			log.error("JsonMappingException,{}", json, e);
		} catch (IOException e) {
			log.error("IOException,{}", json, e);
		}
		return null;
	}
	
	public static ArrayNode StringToArrayNode(String json){
		try {
			ObjectMapper objectMapper = new ObjectMapper();
			ArrayNode arrayNode = (ArrayNode)objectMapper.readTree(json);
			return arrayNode;
		} catch (JsonParseException e) {
			log.error("JsonParseException,{}", json, e);
		} catch (JsonMappingException e) {
			log.error("JsonMappingException,{}", json, e);
		} catch (IOException e) {
			log.error("IOException,{}", json, e);
		}
		return null;
	}
	
	
	public static JsonNode objectToJsonNode(Object obj){
		try {
			ObjectMapper objectMapper = new ObjectMapper();
			String objJson=objectMapper.writeValueAsString(obj);
			JsonNode jsonNode = objectMapper.readTree(objJson);
			return jsonNode;
		} catch (JsonParseException e) {
			log.error("JsonParseException", e);
		} catch (JsonMappingException e) {
			log.error("JsonMappingException", e);
		} catch (IOException e) {
			log.error("IOException", e);
		}
		return null;
	}
	
	public static ObjectNode objectToObjectNode(Object obj){
		try {
			ObjectMapper objectMapper = new ObjectMapper();
			String objJson=objectMapper.writeValueAsString(obj);
			ObjectNode objectNode = (ObjectNode)objectMapper.readTree(objJson);
			return objectNode;
		} catch (JsonParseException e) {
			log.error("JsonParseException", e);
		} catch (JsonMappingException e) {
			log.error("JsonMappingException", e);
		} catch (IOException e) {
			log.error("IOException", e);
		}
		return null;
	}
	
	public static ObjectNode createObjectNode(){
		ObjectMapper objectMapper = new ObjectMapper();
		ObjectNode ObjectNode = objectMapper.createObjectNode();
		return ObjectNode;
	}
	
	public static ArrayNode createArrayNode(){
		ObjectMapper objectMapper = new ObjectMapper();
		ArrayNode ArrayNode = objectMapper.createArrayNode();
		return ArrayNode;
	}
	
	public static ObjectNode createObjectNode(String str){
		ObjectNode ObjectNode=null;
		try {
			ObjectMapper objectMapper = new ObjectMapper();
			ObjectNode = (ObjectNode) objectMapper.readTree(str);
		} catch (Exception e) {
			log.error("Exception.", e);;
		}
		return ObjectNode;
	}
	
	public static ArrayNode createArrayNode(String str){
		ArrayNode ArrayNode=null;
		try {
			ObjectMapper objectMapper = new ObjectMapper();
			ArrayNode = (ArrayNode) objectMapper.readTree(str);
		} catch (Exception e) {
			log.error("Exception.", e);;
		}
		return ArrayNode;
	}
	
	public static String ObjectToStringByReflect(Object obj,String extra){
		StringBuilder sbf = new StringBuilder();
	try {

		Class<?> clz = obj.getClass();
		Field[] fields = clz.getDeclaredFields();
		Field.setAccessible(fields, true);
		int size = fields.length;
		sbf.append("{");
		boolean bool = false;
		for (int i = 0; i < size; i++) {
			String fieldName=fields[i].getName();
			if("serialVersionUID".equals(fieldName)){
				continue;
			}
			sbf.append("\"");
			sbf.append(fieldName);
			sbf.append("\"");
			sbf.append(":");
			Object value = fields[i].get(obj);
			if ((fields[i].getType().equals(String.class) || fields[i].getType().equals(Date.class)) 
					&& value != null && !fieldName.equals("common_cate")) {
				bool = true;
				sbf.append("\"");
			} else {
				bool = false;
			}
			sbf.append(value);
			if (bool) {
				sbf.append("\"");
			}
			if (i != size - 1) {
				sbf.append(",");
			}

		}
		if(extra!=null || !"".equals(extra)){
			sbf.append(",");
			sbf.append(extra);
		}
		sbf.append("}");
	} catch (IllegalArgumentException e) {
		log.error("IllegalArgumentException", e);
	} catch (IllegalAccessException e) {
		log.error("IllegalAccessException", e);
	}
	return sbf.toString();
	}
	
	public static String addChannelAttribute(String json,String childUrl,String url,String parament){
		ArrayNode arrayNode=(ArrayNode)StringToJsonNode(json);
		
		if(arrayNode!=null){
			int size =arrayNode.size();
			ObjectNode objectNode=null;
			for(int i=0;i<size;i++){
				objectNode=(ObjectNode)arrayNode.get(i);
				objectNode.put("childUrl", childUrl+parament+"="+objectNode.get(parament));
				objectNode.put("url", url);
			}
			return arrayNode.toString();
		}
		return json;
	}
	
	
	
	 public static final String ORI_JSON_STR_KEY="ori_json_str";
	    public static JSONObject getData(String src) {
	        JSONObject ret = null;

	        try {
	            JSONObject json = new JSONObject(src);
	            if (json != null) {
	                ret = json.getJSONObject("data");
	            }
	        } catch (Exception e) {
	            log.error("JSON Excp : " + e.getMessage() + "; With src : " + src, e);
	        }

	        return ret;
	    }

	    public static Object getObject(String src, String key) {
	        Object ret = null;

	        try {
	            JSONObject json = new JSONObject(src);
	            if (json != null) {
	                ret = json.getJSONObject("data").get(key);
	            }
	        } catch (Exception e) {
	            log.error("JSON Excp : " + e.getMessage() + "; With src : " + src, e);
	        }

	        return ret;
	    }

	    public static JSONObject getJsonObject(String src, String key) {
	        JSONObject ret = null;

	        try {
	            JSONObject json = new JSONObject(src);
	            if (json != null) {
	                ret = json.getJSONObject("data").getJSONObject(key);
	            }
	        } catch (Exception e) {
	            log.error("JSON Excp : " + e.getMessage() + "; With src : " + src, e);
	        }

	        return ret;
	    }

	    public static JSONArray getJsonArray(String src, String key) {
	        JSONArray ret = null;

	        try {
	            JSONObject json = new JSONObject(src);
	            if (json != null) {
	                ret = json.getJSONObject("data").getJSONArray(key);
	            }
	        } catch (Exception e) {
	            log.error("JSON Excp : " + e.getMessage() + "; With src : " + src, e);
	        }

	        return ret;
	    }

	    @SuppressWarnings("unchecked")
	    public static List<Map> convertJsonArray2MapList(JSONArray jsonArray, boolean withOriJsonStr) {
	        List<Map> ret = null;

	        if (jsonArray != null) {
	            int len = jsonArray.length();
	            ret = new ArrayList<Map>();

	            if(withOriJsonStr){
	                for (int i = 0; i < len; i++) {
	                    try {
	                        JSONObject json = (JSONObject) jsonArray.get(i);
	                        Map e = convertJsonObject2Map(json);
	                        if (e != null) {
	                            e.put(ORI_JSON_STR_KEY, json.toString());
	                            ret.add(e);
	                        }
	                    } catch (Exception e) {
	                        log.error("Exception.", e);
	                    }
	                }
	            } else {
	                for (int i = 0; i < len; i++) {
	                    try {
	                        JSONObject json = (JSONObject) jsonArray.get(i);
	                        Map e = convertJsonObject2Map(json);
	                        if (e != null) {
	                            ret.add(e);
	                        }
	                    } catch (Exception e) {
	                    	log.error("Exception.", e);
	                    }
	                }
	            }
	        }

	        return ret;
	    }

	    @SuppressWarnings("unchecked")
	    public static Map convertJsonObject2Map(JSONObject jsonObject) {
	        Map ret = null;

	        if (jsonObject != null) {
	            ret = new HashMap<Object, Object>();
	            Iterator i = jsonObject.keys();
	            while (i.hasNext()) {
	                String k = (String) i.next();
	                try {
	                    ret.put(k, jsonObject.get(k));
	                } catch (Exception e) {
	                	log.error("Exception.", e);
	                }
	            }
	        }

	        return ret;
	    }

	    @SuppressWarnings("unchecked")
	    public static JSONArray convertCollection2JsonArray(Collection collection) {
	        JSONArray ret = null;

	        if (collection != null && !collection.isEmpty()) {
	            ret = new JSONArray();
	            Iterator i = collection.iterator();
	            while (i.hasNext()) {
	                ret.put(new JSONObject(i.next()));
	            }
	        }

	        return ret;
	    }	

	
	    /**
	     * api接口用的
	     */
	
	    public static String getResponseResult(ObjectMapper objectMapper,JsonNode jsonNode){
	    	ObjectNode resultObjectNode = objectMapper.createObjectNode();
			resultObjectNode.put("status", 200);
			if(jsonNode!=null && jsonNode.size()>0){
				resultObjectNode.put("data", jsonNode);
			}
			resultObjectNode.put("statusText", "OK");
			return resultObjectNode.toString();
	    }
	    
	    /**
	     * api接口用的
	     */
	
	    public static String getResponseResult4Value(ObjectMapper objectMapper,JsonNode jsonNode){
	    	ObjectNode resultObjectNode = objectMapper.createObjectNode();
			resultObjectNode.put("status", 200);
			if(jsonNode != null){
				resultObjectNode.put("data", jsonNode);
			}
			resultObjectNode.put("statusText", "OK");
			return resultObjectNode.toString();
	    }
	    
	    public static boolean jsonNodeIsNull(JsonNode jsonNode){
	    	if(jsonNode==null){
	    		return true;
	    	}
	    	/*String s=jsonNode.asText();
	    	String ss=jsonNode.textValue();
	    	String sss=jsonNode.toString();*/
	    	if( "\"\"".equals(jsonNode.toString()) || "null".equals(jsonNode.toString()) || "".equals(jsonNode.toString()) ){
	    		return true;
	    	}
	    	/*if(("null".equals(jsonNode.asText()) || "".equals(jsonNode.asText())) ){
	    		return true;
	    	}*/
	    	return false;
	    }
	    public static void main(String[] args) {
			ObjectNode objectNode=new ObjectMapper().createObjectNode();
			objectNode.put("int", 444);
			objectNode.put("str", "");
			objectNode.put("str1", "ff");
			objectNode.putPOJO("object", null);
			objectNode.put("node", new ObjectMapper().createObjectNode());
			jsonNodeIsNull(objectNode.get("int"));
			jsonNodeIsNull(objectNode.get("str"));
			jsonNodeIsNull(objectNode.get("str1"));
			jsonNodeIsNull(objectNode.get("object"));
			jsonNodeIsNull(objectNode.get("node"));
		}
}


