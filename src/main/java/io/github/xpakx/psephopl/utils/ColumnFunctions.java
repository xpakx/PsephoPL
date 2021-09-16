package io.github.xpakx.psephopl.utils;

import org.apache.spark.sql.Column;

import static org.apache.spark.sql.functions.*;

public class ColumnFunctions {

    public static Column transformLAU2ToTERC(String columnName) {
        return concat(
                substring(col(columnName),2,2),
                substring(col(columnName),6,4)
        );
    }

    public static Column colToInt(String name) {
        return regexp_replace(
                col(name), " ", "")
                .cast("integer");
    }
}
