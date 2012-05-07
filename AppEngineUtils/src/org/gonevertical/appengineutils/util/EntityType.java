package org.gonevertical.appengineutils.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.google.appengine.api.blobstore.BlobKey;
import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.Category;
import com.google.appengine.api.datastore.Email;
import com.google.appengine.api.datastore.GeoPt;
import com.google.appengine.api.datastore.IMHandle;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Link;
import com.google.appengine.api.datastore.PhoneNumber;
import com.google.appengine.api.datastore.PostalAddress;
import com.google.appengine.api.datastore.Rating;
import com.google.appengine.api.datastore.ShortBlob;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.api.users.User;

public class EntityType {

  public static enum Type {
    /**
     * false < true
     */
    BOOLEAN("BOOLEAN"),
    
    /**
     * byte order, Up to 500 bytes. A value longer than 500 bytes throws a JDOFatalUserException.
     */
    SHORTBLOB("BLOB"),
    
    /**
     * up to 1 megabyte; not indexed
     */
    BLOB("BLOB"),
    
    /**
     * Unicode
     */
    CATEGORY("VARCHAR"),
    
    /**
     * chronological
     */
    DATE("DATETIME"),
    
    /**
     * Unicode
     */
    EMAIL("VARCHAR"),
    
    /**
     * numeric, 64-bit double precision, IEEE 754
     */
    FLOATING("DECIMAL"),
   
    /**
     * by latitude, then longitude
     */
    GEOPT("VARCHAR"),
    
    /**
     * email address in Unicode order
     */
    USER("VARCHAR"),
    
    /**
     * numeric
     */
    INTEGER("INTEGER"),
    
    /**
     * byte order
     */
    BLOBKEY("VARCHAR"),
    
    /**
     * by path elements (kind, ID or name, kind, ID or name...)
     */
    KEY("VARCHAR"),
    
    /**
     * Unicode
     */
    LINK("VARCHAR"),
    
    /**
     * Unicode
     */
    IMHANDLE("VARCHAR"),
    
    /**
     * n/a -I'm doing nothing with this
     */
    NULL("NULL"),
    
    /**
     * Unicode
     */
    POSTALADDRESS("VARCHAR"),
    
    /**
     * numeric
     */
    RATING("VARCHAR"),
    
    /**
     * Unicode
     */
    PHONENUMBER("VARCHAR"),
    
    /**
     * Unicode, Up to 500 Unicode characters. A value longer than 500 characters throws a JDOFatalUserException.
     */
    STRING("TEXT"),
    
    /**
     * up to 1 megabyte; not indexed
     */
    TEXT("TEXT");
    
    private String msyqlType;
    
    Type(String mysqlType) {
      this.msyqlType = mysqlType;
    }
    
    public String getMySqlType() {
      return msyqlType;
    }
    
    public String value() {
      return name();
    }
  }
  
  public static Type getType(Object o) {
    Type t = null;
    if (o == null) {
      t = Type.NULL;
    } else if (o instanceof Boolean) {
      t = Type.BOOLEAN;
    } else if (o instanceof ShortBlob) {
      t = Type.SHORTBLOB;
    } else if (o instanceof Blob) {
      t = Type.BLOB;
    } else if (o instanceof Category) {
      t = Type.CATEGORY;
    } else if (o instanceof Date) {
      t = Type.DATE;
    } else if (o instanceof Email) {
      t = Type.EMAIL;
    } else if (o instanceof Float || o instanceof Double) {
      t = Type.FLOATING;
    } else if (o instanceof GeoPt) {
      t = Type.GEOPT;
    } else if (o instanceof User) {
      t = Type.USER;
    } else if (o instanceof Short || o instanceof Integer || o instanceof Long) {
      t = Type.INTEGER;
    } else if (o instanceof BlobKey) {
      t = Type.BLOBKEY;
    } else if (o instanceof Key) {
      t = Type.KEY;
    } else if (o instanceof Link) {
      t = Type.LINK;
    } else if (o instanceof IMHandle) {
      t = Type.IMHANDLE;
    } else if (o instanceof PostalAddress) {
      t = Type.POSTALADDRESS;
    } else if (o instanceof Rating) {
      t = Type.POSTALADDRESS;
    } else if (o instanceof PhoneNumber) {
      t = Type.PHONENUMBER;
    } else if (o instanceof String) {
      t = Type.STRING;
    } else if (o instanceof Text) {
      t = Type.TEXT;
    } else {
      t = Type.BLOB;
    }
    return t;
  }
  
  public static String getValue(Object o) {
    
    //String v = ObjectUtils.convertObjectToString(o);
    
    String value = null;
    if (o == null) {
      value = null;
      
    } else if (o instanceof Boolean) {
      Boolean b = (Boolean) o;
      if (b == true) { // sql conversion
        value = "1";
      } else {
        value = "0";
      }
      
    } else if (o instanceof ShortBlob) {
      ShortBlob sb = (ShortBlob) o;
      value = sb.toString();
      
    } else if (o instanceof Blob) {
      Blob blob = (Blob) o;
      value = blob.toString();
      
    } else if (o instanceof Category) {
      Category category = (Category) o;
      value = category.toString();
      
    } else if (o instanceof Date) {
      Date date = (Date) o;
      DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      value = df.format(date);
      
    } else if (o instanceof Email) {
      Email email = (Email) o;
      value = email.toString();
    
    } else if (o instanceof Float) {
      Float fl = (Float) o;
      value = fl.toString();
      
    } else if (o instanceof Double) {
      Double dou = (Double) o;
      value = dou.toString();
    
    } else if (o instanceof GeoPt) {
      GeoPt gp = (GeoPt) o;
      value = gp.toString();
    
    } else if (o instanceof User) {
      User user = (User) o;
      value = user.toString();
    
    } else if (o instanceof Short) {
      Short sh = (Short) o;
      value = sh.toString();
      
    } else if (o instanceof Integer) {
      Integer in = (Integer) o;
      value = Integer.toString(in);
      
    } else if (o instanceof Long) {
      Long l = (Long) o;
      value = Long.toString(l);
      
    } else if (o instanceof BlobKey) {
      BlobKey bk = (BlobKey) o;
      value = bk.toString();
    
    } else if (o instanceof Key) {
      Key key = (Key) o;
      value = key.toString();
    
    } else if (o instanceof Link) {
      Link li = (Link) o;
      value = li.toString();
    
    } else if (o instanceof IMHandle) {
      IMHandle im = (IMHandle) o;
      value = im.toString();
      
    } else if (o instanceof PostalAddress) {
      PostalAddress p = (PostalAddress) o;
      value = p.toString();
      
    } else if (o instanceof Rating) {
      Rating rating = (Rating) o;
      value = rating.toString();
      
    } else if (o instanceof PhoneNumber) {
      PhoneNumber phone = (PhoneNumber) o;
      value = phone.toString();
      
    } else if (o instanceof String) {
      String s = (String) o;
      value = s.toString();
      
    } else if (o instanceof Text) {
      Text text = (Text) o;
      value = text.toString();
      
    } else {
      //value = ObjectUtils.convertObjectToString(o);
    }
    return value;
  }
  
}
