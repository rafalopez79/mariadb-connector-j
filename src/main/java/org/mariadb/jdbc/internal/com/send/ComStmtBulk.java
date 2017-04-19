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

Copyright (c) 2009-2011, Marcus Eriksson , Stephane Giron

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

package org.mariadb.jdbc.internal.com.send;

import org.mariadb.jdbc.internal.ColumnType;
import org.mariadb.jdbc.internal.com.Packet;
import org.mariadb.jdbc.internal.com.read.dao.Results;
import org.mariadb.jdbc.internal.com.send.parameters.ParameterHolder;
import org.mariadb.jdbc.internal.io.output.PacketOutputStream;
import org.mariadb.jdbc.internal.protocol.Protocol;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

public class ComStmtBulk {

    /**
     * Write COM_STMT_EXECUTE sub-command to output buffer.
     *
     * @param statementId           prepareResult object received after preparation.
     * @param parametersList        parameters
     * @param parameterCount        parameters number
     * @param pos                   outputStream
     * @throws IOException if a connection error occur
     */
    public static void writeCmd(final int statementId, final List<ParameterHolder[]> parametersList, final int parameterCount,
                                final PacketOutputStream pos, Protocol protocol, Results results) throws IOException, SQLException {
        pos.startPacket(0);
        pos.write(Packet.COM_STMT_BULK_EXECUTE);
        pos.writeInt(statementId);
        short bulkFlags = 192; //Return generated auto-increment IDs + Send types to server
        pos.writeShort(bulkFlags);

        Iterator<ParameterHolder[]> it = parametersList.iterator();
        ParameterHolder[] holders = it.next();
        ColumnType[] parameterTypeHeader = new ColumnType[holders.length];

        //validate parameter set
        if (parameterCount != -1 && parameterCount < holders.length) {
            throw new SQLException("Parameter at position " + (parameterCount - 1) + " is not set", "07004");
        }

        //send type
        for (int i = 0; i < parameterCount; i++) {
            parameterTypeHeader[i] = holders[i].getColumnType();
            pos.writeShort((short) parameterTypeHeader[i].getType());
        }

        while (true) {
            for (int i = 0; i < parameterCount; i++) {
                ParameterHolder holder = holders[i];
                if (holder.isNullData()) {
                    pos.write((byte) 1);
                } else {
                    pos.write((byte) 0);
                    holder.writeBinary(pos);
                }
            }
            if (!it.hasNext()) break;
            holders = it.next();
        }
        pos.flush();

        //read result
        protocol.getResult(results, true);

    }

}
