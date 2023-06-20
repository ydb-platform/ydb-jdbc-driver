package tech.ydb.jdbc.impl.types;

import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;

import javax.sql.rowset.serial.SerialClob;

public class NClobImpl extends SerialClob implements NClob {
    private static final long serialVersionUID = -9026009587491455531L;

    public NClobImpl(char[] ch) throws SQLException {
        super(ch);
    }

    public NClobImpl(Clob clob) throws SQLException {
        super(clob);
    }
}
