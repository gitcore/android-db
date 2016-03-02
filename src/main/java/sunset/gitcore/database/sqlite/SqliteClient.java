package sunset.gitcore.database.sqlite;

import java.math.BigInteger;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

import sunset.gitcore.database.Client;
import sunset.gitcore.database.annotations.Column;
import sunset.gitcore.database.annotations.Indexed;
import sunset.gitcore.database.ColumnMapping;
import sunset.gitcore.database.Log;
import sunset.gitcore.database.NotSupportedException;
import sunset.gitcore.database.TableMapping;
import sunset.gitcore.database.TypeConvert;
import sunset.gitcore.database.Utils;

import android.database.Cursor;
import android.os.Build;

public abstract class SqliteClient implements Client, TypeConvert {
	private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault());
	
    private HashMap<String, TableMapping> _mappings = null;
    
    protected Log log = Log.NONE;
    protected boolean storeDateTimeAsTicks = false;
    
    public abstract void execute(String sql, Object ...bindArgs);
    public abstract Cursor executeQuery(String sql, Object ...bindArgs);
    public abstract long executeInsert(String sql, Object ...bindArgs);
	public abstract long executeUpdateDelete(String sql, Object ...bindArgs);
	//閸忕厧顔�.3
	public abstract long executeUpdate(TableMapping map, Object obj, Class<?> objType);
	//閸忕厧顔�.3
	public abstract long executeDelete(TableMapping map, Object obj);
    
    private TableMapping getMapping(Class<?> cls) {
        if (_mappings == null) {
            _mappings = new HashMap<>();
        }
        
        TableMapping map = null;
        String clsName = cls.getName();
        
        if(!_mappings.containsKey(clsName)) {
            map = new TableMapping(cls);
            _mappings.put(clsName, map);
        } else {
        	map = _mappings.get(clsName);
        }
        return map;
    }
	
	private Object getValue(ColumnMapping mapping, Cursor cursor, int columnIndex) {
        Object val = null;
        
        Class<?> jvmType = mapping.getColumnType();
        
        if (jvmType == int.class) {
        	val = cursor.getInt(columnIndex);
        } else if (jvmType == byte.class) {
        	val = (byte)cursor.getInt(columnIndex);
        } else if (jvmType == long.class) {
        	val = cursor.getLong(columnIndex);
        } else if (jvmType == boolean.class) {
        	val = cursor.getInt(columnIndex) == 1;
        } else if (jvmType == BigInteger.class) {
        	val = cursor.getLong(columnIndex);
        } else if (jvmType == float.class) { 
        	val = cursor.getFloat(columnIndex);
        } else if (jvmType == double.class) {
        	val = cursor.getDouble(columnIndex);
        } else if (jvmType == String.class) {
        	val = cursor.getString(columnIndex);
        } else if (jvmType == Date.class) {
            try {
            	if(storeDateTimeAsTicks) {
            		val = new Date(cursor.getLong(columnIndex));
            	} else {
            		String colVal = cursor.getString(columnIndex);
            		val = Utils.isNullOrEmpty(colVal) ? null : format.parse(colVal);
            	}
			} catch (ParseException e) {
				e.printStackTrace();
			}
        } else if (jvmType.isEnum()) {
        	val = cursor.getInt(columnIndex);
        } else if (jvmType == byte[].class) {
        	val = cursor.getBlob(columnIndex);
        } else if (jvmType == UUID.class) {
        	val = cursor.getString(columnIndex);
        } else if (jvmType == Byte.class) {
        	if(!cursor.isNull(columnIndex)) {
        		val =(byte)cursor.getInt(columnIndex);
        	}
        } else if (jvmType == Integer.class) {
        	if(!cursor.isNull(columnIndex)) {
        		val = cursor.getInt(columnIndex);
        	}
        } else if (jvmType == Long.class) {
        	if(!cursor.isNull(columnIndex)) {
        		val = cursor.getLong(columnIndex);
        	}
        } else if (jvmType == Boolean.class) {
        	if(!cursor.isNull(columnIndex)) {
        		val = cursor.getInt(columnIndex) == 1 ? Boolean.valueOf(true)  : Boolean.valueOf(false);
        	}
        } else if (jvmType == Float.class) {
        	if(!cursor.isNull(columnIndex)) {
        		val = cursor.getFloat(columnIndex);
        	}
        } else if (jvmType == Double.class) {
        	if(!cursor.isNull(columnIndex)) {
            	val = cursor.getDouble(columnIndex);
        	}
        }

        return val;
	}
    
    private Iterable<ColumnInfo> getTableInfo(String name) {
    	Cursor cursor = query(MessageFormat.format(TableMapping.GET_TABLE_INFO_SQL, name));

    	TableMapping map = getMapping(ColumnInfo.class);
    	ColumnMapping[] cols = new ColumnMapping[cursor.getColumnCount()];

        for (int i = 0; i < cols.length; i++) {
            String colName = cursor.getColumnName(i);
            cols[i] = map.findColumn(colName);
        }
        
    	ArrayList<ColumnInfo> list = new ArrayList<>();
        
        try {
            while (cursor.moveToNext()) {
    			try {
    				Object obj = map.getMappedType().newInstance();
                    for (int i = 0; i < cols.length; i++) {
                        if (null == cols[i])
                            continue;
                        
                        cols[i].setValue(obj, getValue(cols[i], cursor, i));
                    }
                    
                    list.add(ColumnInfo.class.cast(obj));
    			} catch (InstantiationException e) {
    				e.printStackTrace();
    			} catch (IllegalAccessException e) {
    				e.printStackTrace();
    			}
            }
        } finally {
        	cursor.close();
        }
        
    	return list;
    }
    

	@Override
	public void setLogger(Log log) {
		this.log = log;
	}
	
    @Override
	public void createTable(Class<?> cls) throws Exception {
        TableMapping map = getMapping(cls);
        
        String query = "create table if not exists " + map.getTableName() + "(\n";

        ArrayList<String> decls = new ArrayList<>();
        for (ColumnMapping col : map.getColumns()) {
        	if (col.isPrimaryKey()) {
            	decls.add(0, getSqlDecl(col, storeDateTimeAsTicks));
        	} else {
        		decls.add(getSqlDecl(col, storeDateTimeAsTicks));
        	}
        }
        
        String decl = Utils.join(",\n", decls.toArray(new String[decls.size()]));
        query += decl;
        query += ")";

        execute(query);
        
        if(Build.VERSION.SDK_INT >= Build.VERSION_CODES.HONEYCOMB) {
            // table already exists, migrate it
            migrateTable(cls);
        }

        //create index, 
        HashMap<String, IndexInfo> indexes = null;
        for (ColumnMapping c : map.getColumns()) {
            for (Indexed i : c.getIndices()) {
            	if (null == indexes) {
            		indexes = new HashMap<>();
            	}
            	
                String iname = Utils.isNullOrEmpty(i.name()) ? map.getTableName() + "_" + c.getName() : i.name();
                IndexInfo iinfo = null;
                
                if(!indexes.containsKey(iname)) {
                    iinfo = new IndexInfo();
                    iinfo.indexName = iname;
                    iinfo.tableName = map.getTableName();
                    iinfo.unique = i.unique();
                    iinfo.columns = new ArrayList<>();
                    indexes.put(iname, iinfo);
                }

                if (i.unique() != iinfo.unique)
                    throw new Exception("All the columns in an index must have the same value for their Unique property");

                IndexedColumn ic = new IndexedColumn();
                ic.order = i.order();
                ic.columnName = c.getName();
                
                iinfo.columns.add(ic);
            }
        }

        if(null != indexes) {
            for (String indexName : indexes.keySet()) {
            	IndexInfo index = indexes.get(indexName);
                String sqlFormat = "create {3} index if not exists {0} on {1}({2})";
                
                Collections.sort(index.columns, new Comparator<IndexedColumn>() {

					@Override
					public int compare(IndexedColumn lhs, IndexedColumn rhs) {
						return Integer.valueOf(lhs.order).compareTo(rhs.order);
					}
					
                });
                
                String[] names = new String[index.columns.size()];
                for (int i = 0; i < names.length; i ++) {
                	names[i] = index.columns.get(i).columnName;
                }
                
                String columns = Utils.join("\",\"", names);
                sqlFormat = MessageFormat.format(sqlFormat, indexName, index.tableName, columns, index.unique ? "unique" : "");
                execute(sqlFormat);
            }
        }
	}

    @Override
    public void migrateTable(Class<?> cls) throws NotSupportedException {
    	TableMapping map = getMapping(cls);
    	
    	Iterable<ColumnInfo> existingCols = getTableInfo(map.getTableName());

    	ArrayList<ColumnMapping> toBeAdded = new ArrayList<>();

        for (ColumnMapping p : map.getColumns()) {
            boolean found = false;
            for (ColumnInfo c : existingCols) {
                found = (p.getName().compareToIgnoreCase(c.name) == 0);
                if (found)
                    break;
            }
            if (!found) {
                toBeAdded.add(p);
            }
        }

        for (ColumnMapping p : toBeAdded) {
            String addCol = "alter table \"" + map.getTableName() + "\" add column " + getSqlDecl(p, storeDateTimeAsTicks);
            execute(addCol);
        }
    }
	
    @Override
    public void dropTable(Class<?> cls) {
    	TableMapping map = getMapping(cls);
        String query = MessageFormat.format("drop table if exists \"{0}\"", map.getTableName());

        execute(query);
    }
    
    public long insert(Object obj) {
        if (obj == null) {
            return 0;
        }
        return insert(obj, Utils.STRING_EMPTY, obj.getClass());
    }
    
    @Override
    public long insert(Object obj, String extra, Class<?> objType) {
        if (obj == null || objType == null) {
            return 0;
        }

        TableMapping map = getMapping(objType);

        boolean replacing = extra.compareToIgnoreCase("OR REPLACE") == 0;
        ColumnMapping[] cols = replacing ? map.getInsertOrReplaceColumns() : map.getInsertColumns();
        
        Object[] vals = new Object[cols.length];
        for (int i = 0; i < vals.length; i++) {
            vals[i] = cols[i].getValue(obj);
        }
        
        String insertSql = null;
        if (null != cols && map.getColumns().length == 1 && map.getColumns()[0].isAutoInc()) {
            insertSql = MessageFormat.format("insert {1} into {0} default values", map.getTableName(), extra);
        } else {
            if (replacing) {
                cols = map.getInsertOrReplaceColumns();
            }
            
            String[] names = new String[cols.length];
            String[] vals2 = new String[cols.length];
            for (int i = 0; i < cols.length; i ++) {
            	names[i] = cols[i].getName();
            	vals2[i] = "?";
            }

            insertSql = MessageFormat.format("insert {3} into {0}({1}) values ({2})", map.getTableName(),
            		Utils.join(",", names),
            		Utils.join(",", vals2), extra);

        }
        
        long lastRowId = executeInsert(insertSql, vals);

        if (map.hasAutoIncPK()) {
            map.setAutoIncPK(obj, lastRowId);
        }

        return lastRowId;
    }
    
    public long insertOrReplace(Object obj) {
        if (obj == null) {
            return 0;
        }
        return insert(obj, "OR REPLACE", obj.getClass());
    }
    
    @Override
    public long insertOrReplace(Object obj, Class<?> objType) {
        return insert(obj, "OR REPLACE", objType);
    }
    
    public long update(Object obj) {
        if (null == obj) {
            return 0;
        }
        return update(obj, Utils.STRING_EMPTY);
    }
    
    @Override
    public long update(Object obj, String where, Object ...args) {
    	if (null == obj) {
            return 0;
        }

    	Class<?> cls = obj.getClass();
    	
        TableMapping map = getMapping(cls);
        
        if (Build.VERSION.SDK_INT <= Build.VERSION_CODES.HONEYCOMB) {
        	return executeUpdate(map, obj, cls);
        }
        
        if(!Utils.isNullOrEmpty(where)) {
            ColumnMapping[] cols = map.getInsertColumns();

//            ArrayList<Object> vals = new ArrayList<>();
//            for(ColumnMapping c : cols) {
//            	vals.add(c.getValue(obj));
//            }
            
            String[] namAry = new String[cols.length];
            for(int i = 0; i < namAry.length; i ++) {
            	namAry[i] = cols[i].getName() + "=? ";
            }
            
            String q = MessageFormat.format("update {0} set {1} where {2} ", map.getTableName(), Utils.join(",", namAry), where);
            
            return executeUpdateDelete(q, args);
        } else {
            ColumnMapping pk = map.getPrimaryKey();

//          if (null == pk) {
//              throw new NotSupportedException("Cannot update " + map.getTableName() + ": it has no PK");
//          }

          ColumnMapping[] cols = map.getInsertColumns();

          ArrayList<Object> vals = new ArrayList<>();
          for(ColumnMapping c : cols) {
          	vals.add(c.getValue(obj));
          }
          ArrayList<Object> ps = new ArrayList<>(vals);
          ps.add(pk.getValue(obj));
          
          String[] namAry = new String[cols.length];
          for(int i = 0; i < namAry.length; i ++) {
          	namAry[i] = cols[i].getName() + "=? ";
          }
          
          String q = MessageFormat.format("update {0} set {1} where {2} = ? ", map.getTableName(), Utils.join(",", namAry), pk.getName());
          
          return executeUpdateDelete(q, ps.toArray()); 
        }
    }
    
    @Override
    public long updateAll(Iterable<Object> objects) {
        int c = 0;
        for (Object r : objects) {
            c += update(r);
        }
        return c;
    }
    
    @Override
    public long delete(Object objectToDelete) throws NotSupportedException {
        TableMapping map = getMapping(objectToDelete.getClass());
        ColumnMapping pk = map.getPrimaryKey();
        if (null == pk) {
            throw new NotSupportedException("Cannot delete " + map.getTableName() + ": it has no PK");
        }
        String q = MessageFormat.format("delete from {0} where {1} = ?", map.getTableName(), pk.getName());
        return executeUpdateDelete(q, pk.getValue(objectToDelete));
    }

    @Override
    public long delete(Class<?> cls, Object primaryKey) throws NotSupportedException {
    	TableMapping map = getMapping(cls);
    	
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.HONEYCOMB) {
        	return executeDelete(map, primaryKey);
        }
    	
    	ColumnMapping pk = map.getPrimaryKey();
        if (null == pk) {
            throw new NotSupportedException("Cannot delete " + map.getTableName() + ": it has no PK");
        }
        String q = MessageFormat.format("delete from {0} where {1} = ?", map.getTableName(), pk.getName());
        return executeUpdateDelete(q, primaryKey);
    }
    
    @Override
    public long delete(Class<?> cls) {
    	TableMapping map = getMapping(cls);
        String query = MessageFormat.format("delete from {0}", map.getTableName());
        
        return executeUpdateDelete(query);
    }
    
    public Cursor query(String query, Object ...args) {
    	return executeQuery(query, args);
    }
    
    @Override
	public <T> T get(Class<T> cls, Object pk) {
    	List<T> list = getList(cls, Utils.STRING_EMPTY, Utils.STRING_EMPTY, pk);
    	return list.size() > 0 ? list.get(0) : null;
    }
    
    @Override
	public <T> T get(Class<T> cls, String where, Object ...args) {
    	List<T> list = getList(cls, where, Utils.STRING_EMPTY, args);
    	return list.size() > 0 ? list.get(0) : null;
    }
    
    @Override
	public <T> List<T> getList(Class<T> cls)
	{
		return getList(cls, Utils.STRING_EMPTY, Utils.STRING_EMPTY);
	}
	
	@Override
	public <T> List<T> getList(Class<T> cls, String orderBy) {
		return getList(cls, Utils.STRING_EMPTY, orderBy);
	}
	
    @Override
    public <T> List<T> getList(Class<T> cls, String where, String orderBy, Object ...args) {
    	return getList(cls, where, Utils.STRING_EMPTY, orderBy, -1, 0, args);
    }
	
    @Override
	 public <T> List<T> getList(Class<T> cls, String where, String groupBy, String orderBy, int pageIndex, int pageSize, Object... args) {
        TableMapping map = getMapping(cls);

        String sqlText = null;

        if(map.isTableView()) {
            sqlText = Utils.isNullOrEmpty(where) ?
                    (null != args && args.length > 0 ? map.getByQuerySql() : map.getByQuerySql()) : map.getByQuerySql()  + " where " + where;
        } else {
            sqlText = Utils.isNullOrEmpty(where) ?
                    (null != args && args.length > 0 ? map.getByPrimaryKeySql() : "select * from "+ map.getTableName()) : MessageFormat.format(map.getByQuerySql(), where);
        }

        if(!Utils.isNullOrEmpty(groupBy)) {
            sqlText += " group by " + groupBy;
        }

        if(!Utils.isNullOrEmpty(orderBy)) {
            sqlText += " order by " + orderBy;
        }

        if(pageIndex > -1 && pageSize > 0) {
            sqlText += " limit "+ (pageIndex * pageSize) +  "," + pageSize;
        }

        Cursor cursor = query(sqlText, args);

        ColumnMapping[] cols = new ColumnMapping[cursor.getColumnCount()];

        for (int i = 0; i < cols.length; i++) {
            String name = cursor.getColumnName(i);
            cols[i] = map.findColumn(name);
        }

//        ArrayList<HashMap<String, Object>> valMapList = new ArrayList<HashMap<String,Object>>();

        ArrayList<T> list = new ArrayList<>();

        try {
            while (cursor.moveToNext()) {
                try {
                    Object obj = map.getMappedType().newInstance();
                    for (int i = 0; i < cols.length; i++) {
                        if (null == cols[i])
                            continue;

//                        HashMap<String, Object> valMap = new HashMap<String, Object>();
//
//                        valMap.put(cols[i].getName(), cursor.get)

                        cols[i].setValue(obj, getValue(cols[i], cursor, i));
                    }

                    list.add(cls.cast(obj));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } finally {
            cursor.close();
        }

        return list;
	 }

	 @Override
	    public String getSqlDecl(ColumnMapping p, boolean storeDateTimeAsTicks) throws NotSupportedException {
	        String decl = "\"" + p.getName() + "\" " + getSqlType(p, storeDateTimeAsTicks) + " ";

	        if (p.isPrimaryKey()) {
	            decl += "primary key ";
	        }
	        if (p.isAutoInc()) {
	            decl += "autoincrement ";
	        }
	        if (!p.isNullable()) {
	            decl += "not null ";
	        }
	        if (!Utils.isNullOrEmpty(p.getCollation())) {
	            decl += "collate " + p.getCollation() + " ";
	        }

	        return decl;
	    }

	    @Override
	    public String getSqlType(ColumnMapping p, boolean storeDateTimeAsTicks) throws NotSupportedException {
	        Class<?> jvmType = p.getColumnType();
	      
	        if (jvmType == boolean.class || jvmType == byte.class || jvmType == int.class || jvmType == long.class || jvmType == Boolean.class ||
	        		jvmType == Boolean.class || jvmType == Byte.class || jvmType == Integer.class || jvmType == Long.class) {
	            return "integer";
	        } else if (jvmType == BigInteger.class) {
	            return "bigint";
	        } else if (jvmType == float.class || jvmType == Float.class) {
	            return "float";
	        } else if (jvmType == double.class || jvmType == Double.class) {
	            return "double";
	        } else if (jvmType == String.class) {
	            int len = p.getMaxStringLength();
	            return "varchar(" + len + ")";
	        } else if (jvmType == Date.class) {
	            return storeDateTimeAsTicks ? "bigint" : "datetime";
	        } else if (jvmType.isEnum()) {
	            return "integer";
	        } else if (jvmType == byte[].class) {
	            return "blob";
	        } else if (jvmType == UUID.class) {
	            return "varchar(36)";
	        } else {
	            throw new NotSupportedException("Don't know about " + jvmType.getName());
	        }
	    }

    private static class IndexedColumn {
        public int order;
        public String columnName;
    }

    private static class IndexInfo {
    	@SuppressWarnings("unused")
		public String indexName;
    	public String tableName;
    	public boolean unique;
    	public ArrayList<IndexedColumn> columns;
    }
    
    
    public static class ColumnInfo {
        @Column
        private String name;

//		private int cid;
//		private String type;
//		private int notnull;
//		private String dflt_value;
//		private int pk;
        
        public String getName() {
        	return name;
        }
        
        public void setName(String value) {
        	name = value;
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
