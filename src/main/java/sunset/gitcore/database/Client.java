package sunset.gitcore.database;

import java.util.List;

public interface Client
{
		void createTable(Class<?> cls) throws Exception;
	    void migrateTable(Class<?> cls)  throws NotSupportedException;
	    void dropTable(Class<?> cls);
	    long insert(Object obj);
	    long insert(Object obj, String extra, Class<?> objType);
	    long insertOrReplace(Object obj);
	    long insertOrReplace(Object obj, Class<?> objType);
	    long update(Object obj);
	    long update(Object obj, String where, Object ...args);
	    long updateAll(Iterable<Object> objects);
	    long delete(Object objectToDelete) throws NotSupportedException;
	    long delete(Class<?> cls, Object primaryKey) throws NotSupportedException;
	    long delete(Class<?> cls) throws NotSupportedException;
	    <T> T get(Class<T> cls, Object pk);
		<T> T get(Class<T> cls, String where, Object ...args);
		<T> List<T> getList(Class<T> cls);
		<T> List<T> getList(Class<T> cls, String orderBy);
	    <T> List<T> getList(Class<T> cls, String where, String orderBy, Object ...args);
	    <T> List<T> getList(Class<T> cls, String where, String groupBy, String orderBy, int pageIndex, int pageSize, Object ...args);
	    
	    void setLogger(Log log);
}
