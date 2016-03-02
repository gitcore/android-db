package sunset.gitcore.database;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Locale;

import sunset.gitcore.database.annotations.AutoIncrement;
import sunset.gitcore.database.annotations.Collation;
import sunset.gitcore.database.annotations.ForeignKey;
import sunset.gitcore.database.annotations.Indexed;
import sunset.gitcore.database.annotations.MaxLength;
import sunset.gitcore.database.annotations.PrimaryKey;
import sunset.gitcore.database.annotations.TableView;

import android.annotation.SuppressLint;

public final class Utils {
	public static final String STRING_EMPTY = "";
	public static final int DEFAULT_MAX_STRING_LENGTH = 140;
	
    public static boolean isNullOrEmpty(String inVal) {
        return (inVal == null || STRING_EMPTY.equals(inVal));
    }
    
    public static String join(String join, String[] strAry) {
    	StringBuffer sb = new StringBuffer();
    	for(int i = 0; i < strAry.length; i ++) {
    		if(i == (strAry.length - 1)) {
    			sb.append(strAry[i]);
    		} else {
    			sb.append(strAry[i]).append(join);
    		}
    	}
    	
    	return new String(sb);
    }
    
	public static String dateFormat(Date date, String format) {
        SimpleDateFormat df = new SimpleDateFormat(format, Locale.getDefault());
        return df.format(date);
    }
    
    @SuppressLint("DefaultLocale")
	public static String parseGetMethodName(String fieldName) { 
    	
        if (null == fieldName || STRING_EMPTY.equals(fieldName)) { 
            return null; 
        } 

        return "get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1); 

    } 

    @SuppressLint("DefaultLocale")
	public static String parseSetMethodName(String fieldName) { 

        if (null == fieldName || STRING_EMPTY.equals(fieldName)) { 
            return null; 
        } 

        return "set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1); 
    }
    
    @SuppressLint("DefaultLocale")
	public static String parseBooleanGetMethodName(String fieldName) { 
    	
        if (null == fieldName || STRING_EMPTY.equals(fieldName)) { 
            return null; 
        } 

        return fieldName.startsWith("is") ? fieldName : "is" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1); 
    } 
    
    @SuppressLint("DefaultLocale")
	public static String parseBooleanSetMethodName(String fieldName) { 

        if (null == fieldName || STRING_EMPTY.equals(fieldName)) { 
            return null; 
        } 

        return fieldName.startsWith("is") ? "set" + fieldName.substring(2) : "set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1); 
    }
    
    public static boolean isPrimaryKey(Field p) {
    	PrimaryKey attrs = p.getAnnotation(PrimaryKey.class);
    	
    	return (null != attrs);
    }
    
    public static boolean isForeignKey(Method p) {
    	ForeignKey attrs = p.getAnnotation(ForeignKey.class);
    	
    	return (null != attrs);
    }

    public static String getCollation(Field p) {
    	Collation attrs = p.getAnnotation(Collation.class);
    	
    	if(null != attrs) {
    		return attrs.value();
    	} else {
    		return Utils.STRING_EMPTY;
    	}
    }

    public static boolean isAutoInc(Field p) {
    	AutoIncrement attrs = p.getAnnotation(AutoIncrement.class);
    	
    	return (null != attrs);
    }
    
    public static boolean isTableView(Class<?> cls) {
    	TableView attrs = cls.getAnnotation(TableView.class);
    	
    	return (null != attrs);
    }

    public static Iterable<Indexed> getIndices(Field p) {
    	ArrayList<Indexed> result = new ArrayList<Indexed>();
        for (Annotation annotation : p.getAnnotations()) {    
            if (annotation instanceof Indexed) {  
                Indexed index = (Indexed) annotation;
                result.add(index);
            }  
        }  
        return result;
    }

    public static int getMaxStringLength(Field p) {
    	MaxLength attrs = p.getAnnotation(MaxLength.class);
    	
    	if(null != attrs) {
    		return attrs.value();
    	} else {
    		return DEFAULT_MAX_STRING_LENGTH;
    	}
    }
}
