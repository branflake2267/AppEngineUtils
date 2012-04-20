package org.gonevertical.appengineutils.util;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class FileNamingUtil {
  
  public static String getFileName(String kind, Date date) {
    String sdate = getDateTime(date);
    String name = cleanDate(kind, sdate);
    return name;
  }
  
  private static String getDateTime(Date date) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String s = df.format(calendar.getTime());
    return s;
  }
  
  private static String cleanDate(String kind, String sdate) {
    sdate = sdate.replaceAll(" ", "__");
    sdate = sdate.replaceAll(":", "-");
    String name = sdate + "___" + kind;
    return name;
  }

}
