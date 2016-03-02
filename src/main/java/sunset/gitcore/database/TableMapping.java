package sunset.gitcore.database;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.text.MessageFormat;
import java.util.ArrayList;

import sunset.gitcore.database.annotations.Ignore;
import sunset.gitcore.database.annotations.Table;
import sunset.gitcore.database.annotations.TableView;

public class TableMapping {
	public static final String GET_TABLE_INFO_SQL = "pragma table_info(\"{0}\")";
	
    ColumnMapping _autoPk = null;

    private boolean isView;
    private Class<?> mappedType;
    private String tableName;
    private ColumnMapping[] columns;
    private ColumnMapping pk;
    private ColumnMapping[] fk;
    private String getByPrimaryKeySql;
    private String getByQuerySql;
    private boolean hasAutoIncPK;
    
    public TableMapping(Class<?> type) {
        mappedType = type;

        if(Utils.isTableView(type)) {
        	isView = true;
        	TableView tableAttr = type.getAnnotation(TableView.class);
            getByQuerySql = tableAttr.query();
        } else {
            Table tableAttr = type.getAnnotation(Table.class);
            tableName = tableAttr != null ? tableAttr.name() : mappedType.getName();
            getByQuerySql = "select * from "+ tableName + " where {0}";
        }

        ArrayList<ColumnMapping> tmp = new ArrayList<>();
        
        Field[] props = mappedType.getDeclaredFields();

        for (Field p : props) {
        	if(Modifier.isStatic(p.getModifiers())) continue;
        	
        	Ignore ignore = p.getAnnotation(Ignore.class);

        	if (null == ignore) {
        		tmp.add(new ColumnMapping(p));
        	}
        }
        
        columns = new ColumnMapping[tmp.size()];
        
        tmp.toArray(columns);
        tmp.clear();
        
        for (ColumnMapping c : columns) {
        	if(c.isPrimaryKey()) {
        		pk = c;
        		
        		if(c.isAutoInc()) {
        			_autoPk = c;
        		}
        	}
        }
        
        fk = new ColumnMapping[tmp.size()];
        tmp.toArray(fk);
        tmp.clear();

        if(!Utils.isTableView(type)) {
            hasAutoIncPK = _autoPk != null;

            if (pk != null) {
                getByPrimaryKeySql = MessageFormat.format("select * from {0} where \"{1}\" = ?", tableName, pk.getName());
            } else {
                getByPrimaryKeySql = MessageFormat.format("select * from {0} limit 1", tableName);
            }
        }
    }
    
    public boolean isTableView() {
    	return isView;
    }
    
    public Class<?> getMappedType() {
    	return mappedType;
    }
    
    public String getTableName() {
    	return tableName;
    }
    
    public ColumnMapping[] getColumns() {
    	return columns;
    }
    
    public ColumnMapping getPrimaryKey() {
    	return pk;
    }
    
    public String getByPrimaryKeySql() {
    	return getByPrimaryKeySql;
    }
    
    public String getByQuerySql() {
    	return getByQuerySql;
    }

    public boolean hasAutoIncPK() {
    	return hasAutoIncPK;
    }
    
    public ColumnMapping[] getInsertColumns() {
        ArrayList<ColumnMapping> cols = new ArrayList<>();
        for(ColumnMapping p : columns) {
        	if(!p.isPrimaryKey() || !p.isAutoInc()) {
        		cols.add(p);
        	}
        }
        
        return cols.toArray(new ColumnMapping[cols.size()]);
    }

    public ColumnMapping[] getInsertOrReplaceColumns() {
        return columns;
    }

    public void setAutoIncPK(Object obj, long id) {
        if (_autoPk != null) {
            _autoPk.setValue(obj, id);
        }
    }

    public ColumnMapping findColumnWithPropertyName(String propertyName) {
    	ColumnMapping exact = null;
    	
    	for (ColumnMapping c : columns) {
    		if (c.getProperyName() == propertyName) {
    			exact = c;
    			break;
    		}
    	}
        return exact;
    }
    
    public ColumnMapping findColumn(String columnName) {
    	for(ColumnMapping col : columns) {
    		if(columnName.equals(col.getName())) {
    			return col;
    		}
    	}
    	
        return null;
    }
    
    public int findColumnIndex(String columnName) {
    	for(int i = 0; i < columns.length; i ++) {
    		if(columns[i].getName() == columnName) {
    			return i;
    		}
    	}
    	
        return -1;
    }
}
