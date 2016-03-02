package sunset.gitcore.database;

/**
 * Created by sunyu on 14-3-19.
 */
public interface TypeConvert {
    String getSqlDecl(ColumnMapping p, boolean storeDateTimeAsTicks) throws NotSupportedException;
    String getSqlType(ColumnMapping p, boolean storeDateTimeAsTicks) throws NotSupportedException;
}
