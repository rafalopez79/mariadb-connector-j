package org.mariadb.jdbc.internal.queryresults;

import org.mariadb.jdbc.internal.queryresults.resultset.MariaSelectResultSet;

import java.sql.SQLException;

public interface MultiExecutionResult extends ExecutionResult {

    int[] updateResultsForRewrite(int waitedSize, boolean hasException);

    int[] updateResultsMultiple(int waitedSize, boolean hasException);

    int[] getAffectedRows();

    int getFirstAffectedRows();

    void addResultSetStat(MariaSelectResultSet result, boolean moreResultAvailable) throws SQLException;
}

