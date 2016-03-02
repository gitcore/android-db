package sunset.gitcore.database;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;

import sunset.gitcore.database.annotations.Column;
import sunset.gitcore.database.annotations.Indexed;

public class ColumnMapping {
    private Field _prop;
    private Method _propGet;
    private Method _propSet;

    private String name;
    private Class<?> columnType;
    private String collation;
    private boolean isAutoInc;
    private boolean isPK;
    private Iterable<Indexed> indices;
    private boolean isNullable;
    private int maxStringLength;
    private HashMap<String, Object> extensionAttrs;

    public ColumnMapping(Field prop) {
        Column colAttr = prop.getAnnotation(Column.class);

        name = colAttr != null && !Utils.isNullOrEmpty(colAttr.name()) ? colAttr.name() : prop.getName();
        columnType = prop.getType();
        collation = Utils.getCollation(prop);
        isAutoInc = Utils.isAutoInc(prop);
        isPK = Utils.isPrimaryKey(prop);
        indices = Utils.getIndices(prop);
        isNullable = !isPK;
        maxStringLength = Utils.getMaxStringLength(prop);
        
        _prop = prop;
        
    	boolean isBoolean = columnType.equals(boolean.class);
    	
        try {
			_propSet = isBoolean ? _prop.getDeclaringClass().getDeclaredMethod(
					Utils.parseBooleanSetMethodName(_prop.getName()),  _prop.getType()) :  _prop.getDeclaringClass().getDeclaredMethod(
					Utils.parseSetMethodName(_prop.getName()),  _prop.getType());					
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		}
        
        try {					
			_propGet = isBoolean ? _prop.getDeclaringClass().getDeclaredMethod(
					Utils.parseBooleanGetMethodName(_prop.getName()))  : _prop.getDeclaringClass().getDeclaredMethod(
					Utils.parseGetMethodName(_prop.getName()));
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		}
    }
    
    public String getName() {
    	return name;
    }
    
    public String getProperyName() {
    	return _prop.getName();
    }
    
    public Class<?> getColumnType() {
    	return columnType;
    }
    
    public String getCollation() {
    	return collation;
    }
    
    public boolean isAutoInc() { return isAutoInc; }
    
    public boolean isPrimaryKey() { return isPK; }
    
    public Iterable<Indexed> getIndices() {
    	return indices;
    }
    
    public boolean isNullable() {
		return isNullable;
	}
    
    public int getMaxStringLength() {
    	return maxStringLength;
    }
    
    public Object getExtensionAttrs(String key) {
    	return extensionAttrs.get(key);
    }
    
    public void setExtensionAttrs(String key, Object value) {
    	extensionAttrs.put(key, value);
    }

    public void setValue(Object obj, Object val) {
		try {
			if(null == val) {
				return;
			}
			
			if (null != _propSet) {
				_propSet.invoke(obj, val);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
    }

    public Object getValue(Object obj) {
		try {
			if (null != _propGet) {
				return _propGet.invoke(obj);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
        return null;
    }
}
