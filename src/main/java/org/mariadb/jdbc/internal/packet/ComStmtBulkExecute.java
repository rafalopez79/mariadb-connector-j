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

package org.mariadb.jdbc.internal.packet;

import org.mariadb.jdbc.internal.MariaDbType;
import org.mariadb.jdbc.internal.packet.dao.parameters.ParameterHolder;
import org.mariadb.jdbc.internal.stream.MaxAllowedPacketException;
import org.mariadb.jdbc.internal.stream.PacketOutputStream;

import java.io.IOException;
import java.util.List;

public class ComStmtBulkExecute {

    /**
     * Write COM_STMT_EXECUTE sub-command to output buffer.
     *
     * @param statementId         prepareResult object received after preparation.
     * @param parametersList      parameters List
     * @param writer outputStream
     * @param counterOffset counter offset
     * @param parameterSize parameter total size
     * @param lastSendTypes last sent data types
     * @return ending counter offset
     * @throws IOException if a connection error occur
     */
    public static int writeCmd(final int statementId, final List<ParameterHolder[]> parametersList,
                               final PacketOutputStream writer, int counterOffset, int parameterSize,
                               MariaDbType[] lastSendTypes) throws IOException {

        writer.startPacket(0);

        writer.buffer.put(Packet.COM_STMT_EXECUTE);
        writer.buffer.putInt(statementId);
        writer.buffer.put((byte) 0x00); //CURSOR TYPE NO CURSOR
        //reserve 4 byte for Iteration count
        writer.buffer.position(writer.buffer.position() + 4);

        ParameterHolder[] parameters;
        boolean mustSendHeaderType = true;
        int parameterCount = parametersList.get(0).length;
        int initialCounterOffset = counterOffset;
        int parameterBufferSize;
        boolean hasLongData = false;

        while (!hasLongData && counterOffset < parameterSize) {
            parameters = parametersList.get(counterOffset);

            //create null bitmap
            if (parameterCount > 0) {
                if (mustSendHeaderType) {
                    writer.assureBufferCapacity(1 + parameterCount * 2);
                    writer.buffer.put((byte) 0x01);
                    //Store types of parameters in first in first package that is sent to the server.
                    for (int i = 0; i < parameterCount; i++) {
                        lastSendTypes[i] = parameters[i].getMariaDbType();
                        //16384 indicate that one byte indicate if null or not before
                        writer.buffer.putShort((short) (parameters[i].getMariaDbType().getType() | 16384));
                    }
                    mustSendHeaderType = false;
                } else {
                    for (int i = 0; i < parameterCount; i++) {
                        if (lastSendTypes[i] != parameters[i].getMariaDbType() && !parameters[i].isNullData()) {
                            //data type have changed -> must send current buffer, and send current row as a new packet.
                            return setIterationCountAndSend(counterOffset, initialCounterOffset, writer, parametersList, statementId);
                        }
                    }
                }
            }

            //ensure that buffer size is big enough
            parameterBufferSize = 0;
            for (int i = 0; i < parameterCount; i++) {
                if (parameters[i].isLongData()) {
                    hasLongData = true;
                } else {
                    if (parameters[i].isNullData()) {
                        parameterBufferSize++;
                    } else {
                        parameterBufferSize += parameters[i].getApproximateBinaryProtocolLength() + 1;
                    }
                }
            }

            //send Row data
            if (!hasLongData) {
                if (writer.belowMaxAllowedPacket(parameterBufferSize)) {
                    writer.assureBufferCapacity(parameterBufferSize);
                    for (int i = 0; i < parameterCount; i++) {
                        if (parameters[i].isNullData()) {
                            writer.buffer.put((byte) 1); //STMT_INDICATOR_NULL
                        } else {
                            writer.buffer.put((byte) 0); //STMT_INDICATOR_NONE
                            parameters[i].writeUnsafeBinary(writer);
                        }
                    }
                    counterOffset++;

                } else {

                    //handle case when one row size > max_allowed_packet size
                    if (counterOffset == initialCounterOffset) {
                        throw new MaxAllowedPacketException("stream size " + (parameterBufferSize + writer.buffer.position())
                                + " is >= to max_allowed_packet (" + writer.getMaxAllowedPacket() + ")", false);
                    }

                    return setIterationCountAndSend(counterOffset, initialCounterOffset, writer, parametersList, statementId);
                }
            }

        }


        if (hasLongData) {
            //send rows without long data
            if (counterOffset - initialCounterOffset > 0) {
                return -1 * setIterationCountAndSend(counterOffset, initialCounterOffset, writer, parametersList, statementId);
            }
            return 0;
        } else return setIterationCountAndSend(counterOffset, initialCounterOffset, writer, parametersList, statementId);

    }

    private static int setIterationCountAndSend(final int counterOffset, final int initialCounterOffset,
                                                final PacketOutputStream writer, List<ParameterHolder[]> parametersList,
                                                final int statementId) throws IOException {
        int batchLength = counterOffset - initialCounterOffset;

        if (batchLength == 1) {
            ParameterHolder[] parameters = parametersList.get(counterOffset - 1);
            writer.startPacket(0, true);
            ComStmtExecute.writeCmd(statementId, parameters, parameters.length, new MariaDbType[parameters.length], writer);
            writer.finishPacketWithoutRelease(true);
            return counterOffset;
        }
        writer.buffer.array()[10] = (byte) (batchLength & 0xff);
        writer.buffer.array()[11] = (byte) (batchLength >>> 8);
        writer.buffer.array()[12] = (byte) (batchLength >>> 16);
        writer.buffer.array()[13] = (byte) (batchLength >>> 24);
        writer.finishPacketWithoutRelease(true);
        return counterOffset;
    }
}
