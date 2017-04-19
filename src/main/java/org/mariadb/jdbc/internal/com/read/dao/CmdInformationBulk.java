package org.mariadb.jdbc.internal.com.read.dao;

/*
MariaDB Client for Java

Copyright (c) 2012-2014 Monty Program Ab.

This library is free software; you can redistribute it and/or modify it under
the terms of the GNU Lesser General Public License as published by the Free
Software Foundation; either version 2.1 of the License, or (at your option)
any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
for more details.

You should have received a copy of the GNU Lesser General Public License along
with this library; if not, write to Monty Program Ab info@montyprogram.com.

This particular MariaDB Client for Java file is work
derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
the following copyright and notice provisions:

Copyright (c) 2009-2011, Marcus Eriksson

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:
Redistributions of source code must retain the above copyright notice, this list
of conditions and the following disclaimer.

Redistributions in binary form must reproduce the above copyright notice, this
list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

Neither the name of the driver nor the names of its contributors may not be
used to endorse or promote products derived from this software without specific
prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/

import org.mariadb.jdbc.internal.com.read.resultset.SelectResultSet;
import org.mariadb.jdbc.internal.protocol.Protocol;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class CmdInformationBulk implements CmdInformation {

    private Queue<RowResult> rowResults;
    private Queue<Long> updateCounts;
    private boolean hasException;


    /**
     * CmdInformationBulk store information result dedicated to bulk results
     *
     * @param updateCount  update count
     */
    public CmdInformationBulk(long updateCount) {
        rowResults = new ConcurrentLinkedQueue<>();
        updateCounts = new ConcurrentLinkedQueue<>();
        updateCounts.add(updateCount);
    }

    @Override
    public void addErrorStat() {
        hasException = true;
    }

    public void addResultSetStat() {
        //do nothing, bulk cannot have result-set other than insert ids.
    }

    @Override
    public void addSuccessStat(long updateCount, long insertId) {
        updateCounts.add(updateCount);
    }

    @Override
    public void addBulkResult(SelectResultSet resultSet) {
        try {
            while (resultSet.next()) {
                rowResults.add(new RowResult(resultSet.getLong(1),
                        resultSet.getLong(2),
                        resultSet.getLong(3)));
            }
        } catch (SQLException sqle) {
            //cannot occur
        }
    }

    @Override
    public int[] getUpdateCounts() {
        long size = 0;
        for (long updateCount : updateCounts) size += updateCount;
        int[] ret = new int[(int) size];

        Arrays.fill(ret, hasException ? Statement.EXECUTE_FAILED : Statement.SUCCESS_NO_INFO);
        return ret;
    }

    @Override
    public long[] getLargeUpdateCounts() {
        long size = 0;
        for (long updateCount : updateCounts) size += updateCount;
        long[] ret = new long[(int) size];

        Arrays.fill(ret, hasException ? Statement.EXECUTE_FAILED : Statement.SUCCESS_NO_INFO);
        return ret;
    }

    /**
     * Will return an array filled with Statement.EXECUTE_FAILED if any error occur,
     * or Statement.SUCCESS_NO_INFO, if execution succeed.
     *
     * @return update count array.
     */
    public int[] getRewriteUpdateCounts() {
        //no rewrite
        return null;
    }

    /**
     * Same than getRewriteUpdateCounts, returning long array.
     * @return update count array.
     */
    public long[] getRewriteLargeUpdateCounts() {
        //no rewrite
        return null;
    }

    @Override
    public int getUpdateCount() {
        long updateCountTotal = 0;
        boolean hasUpdate = false;
        for (long updateCount : updateCounts) {
            updateCountTotal += updateCount;
            hasUpdate = true;
        }
        return (!hasUpdate) ? - 1 : (int) updateCountTotal;
    }

    @Override
    public long getLargeUpdateCount() {
        long updateCountTotal = 0;
        boolean hasUpdate = false;
        for (long updateCount : updateCounts) {
            updateCountTotal += updateCount;
            hasUpdate = true;
        }
        return (!hasUpdate) ? - 1 : updateCountTotal;
    }

    @Override
    public ResultSet getBatchGeneratedKeys(Protocol protocol) {
        long size = 0;
        for (RowResult rowResult : rowResults) {
            size += rowResult.len;
        }
        long[] ret = new long[(int) size];

        Iterator<RowResult> rowResultsIter = rowResults.iterator();
        int pos = 0;
        while (rowResultsIter.hasNext()) {
            RowResult rowResult = rowResultsIter.next();
            for (int i = 0; i < rowResult.len; i++) {
                ret[pos++] = rowResult.id + (i * rowResult.autoincrement);
            }
        }

        return SelectResultSet.createGeneratedData(ret, protocol, true);
    }

    /**
     * Return GeneratedKeys containing insert ids.
     * Insert ids are calculated using autoincrement value.
     *
     * @param protocol current protocol
     * @return a resultSet with insert ids.
     */
    public ResultSet getGeneratedKeys(Protocol protocol) {
        //not called
        return null;
    }

    public int getCurrentStatNumber() {
        return updateCounts.size();
    }


    @Override
    public boolean moreResults() {
        return false;
    }

    @Override
    public boolean isCurrentUpdateCount() {
        return false;
    }

    private class RowResult {
        public long id;
        public long len;
        public long autoincrement;

        public RowResult(long id, long len, long autoincrement) {
            this.id = id;
            this.len = len;
            this.autoincrement = autoincrement;
        }
    }
}

