package org.gonevertical.appengineutils.util;

import com.google.gson.Gson;

public class GsonUtils {
  
  private static Gson gson;

  public static String convertObjectToString(Object o) {
    if (gson == null) {
      gson = new Gson();
    }
    String s = gson.toJson(o);
    return s;
  }

  public static <T> T stringToObject(Class<T> clazz, String json) {
    if (gson == null) {
      gson = new Gson();
    }
    T o = gson.fromJson(json, clazz);
    return o;
  }
  
} 



