package sunset.gitcore.android.database.sqlite;

import java.util.Date;

import net.sqlcipher.Cursor;
import net.sqlcipher.database.SQLiteDatabase;
import net.sqlcipher.database.SQLiteStatement;

import sunset.gitcore.database.TableMapping;
import sunset.gitcore.database.Utils;
import sunset.gitcore.database.sqlite.SqliteClient;

import android.annotation.SuppressLint;

public final class SqlCipherClient extends SqliteClient {
    private final SQLiteDatabase db;
    
    public SqlCipherClient(SQLiteDatabase db) {
    	this.db = db;
    }
    
    public void execute(String sql, Object ...bindArgs) {
    	SQLiteStatement stmt = db.compileStatement(sql);
    	
    	final int count = bindArgs != null ? bindArgs.length : 0;
    	bindParameters(stmt, bindArgs, count, storeDateTimeAsTicks);
    	
    	stmt.execute();
    	stmt.close();
    }
    
    public Cursor executeQuery(String sql, Object ...bindArgs) {
    	String[] selectionArgs = null;
    	
    	if(null != bindArgs) {
    		selectionArgs = new String[bindArgs.length];
        	for (int i = 0; i < selectionArgs.length; i ++) {
        		if(bindArgs[i] instanceof Boolean) {
        			selectionArgs[i] = (Boolean)bindArgs[i] ? String.valueOf(1) : String.valueOf(0);
        		} else {
            		selectionArgs[i] = bindArgs[i].toString();
        		}
        	}
    	}
    	
    	log.log(sql);
    	log.log(bindArgs);
    	
    	return db.rawQuery(sql, selectionArgs);
    }
    
    public long executeInsert(String sql, Object ...bindArgs) {
    	SQLiteStatement stmt = db.compileStatement(sql);
    	
    	final int count = bindArgs != null ? bindArgs.length : 0;
    	bindParameters(stmt, bindArgs, count, storeDateTimeAsTicks);
    	
    	long ret = stmt.executeInsert();
    	stmt.close();
    	
    	log.log(sql);
    	log.log(bindArgs);
    	
    	return ret;
    }
    
	@SuppressLint("NewApi")
	public long executeUpdateDelete(String sql, Object ...bindArgs) {
		SQLiteStatement stmt = db.compileStatement(sql);
    	
    	final int count = bindArgs != null ? bindArgs.length : 0;
    	bindParameters(stmt, bindArgs, count, storeDateTimeAsTicks);
    	
    	long ret = stmt.executeUpdateDelete();
    	stmt.close();
    	
    	log.log(sql);
    	log.log(bindArgs);
    	
    	return ret;
    }

	@Override
	public long executeUpdate(TableMapping map, Object obj, Class<?> objType) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long executeDelete(TableMapping map, Object obj) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	private static void bindParameters(SQLiteStatement stmt, Object[] bindArgs, int count, boolean storeDateTimeAsTicks) {
    	for (int i = 0; i < count; i ++) {
    		bindParameter(stmt, i+1, bindArgs[i], storeDateTimeAsTicks);
    	}
	}
	
	private static void bindParameter(SQLiteStatement stmt, int index, Object value, boolean storeDateTimeAsTicks) {        
        if (value == null) {
        	stmt.bindNull(index);
        } else {
        	if (value instanceof byte[]) {
            	stmt.bindBlob(index, (byte[])value);
            } else if (value instanceof Boolean) {
            	stmt.bindLong(index, ((Boolean)value) ? 1 : 0);
            } else if (value instanceof Float || value instanceof Double) {
            	stmt.bindDouble(index, ((Number)value).doubleValue());
            } else if (value instanceof Long || value instanceof Integer
                    || value instanceof Short || value instanceof Byte) {
            	stmt.bindLong(index, ((Number)value).longValue());
            } else if (value instanceof Date) {
                if (storeDateTimeAsTicks) {
                	stmt.bindLong(index, ((Date)value).getTime());
                } else {
                	stmt.bindString(index, Utils.dateFormat((Date)value, "yyyy-MM-dd HH:mm:ss"));
                }
            } else {
            	stmt.bindString(index, value.toString());
            }
        }
    }
}
