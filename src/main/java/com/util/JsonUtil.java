package com.util;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtil {
	private static ObjectMapper mapper = new ObjectMapper();
	
	 public static <T> String getJson(T object) throws JsonProcessingException{
		 return mapper.writeValueAsString(object);
	}
	 
	 public static <T> T getBean(String json,Class<T> clazz ) throws JsonParseException, JsonMappingException, IOException{
		 // vo里属性不存在时忽略,不报错
	    mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
	    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	  return mapper.readValue( json, clazz);
	 }

	public static ObjectMapper getMapper() {
		return mapper;
	}

	public static void setMapper(ObjectMapper mapper) {
		JsonUtil.mapper = mapper;
	}
	 
	 
	 
}
