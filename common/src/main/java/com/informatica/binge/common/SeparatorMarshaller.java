/* 
 * Copyright 2014 Informatica Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.informatica.binge.common;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.informatica.um.binge.api.impl.VDSEventImpl;
import com.informatica.um.binge.api.impl.VDSEventListImpl;
import com.informatica.vds.api.VDSConfiguration;
import com.informatica.vds.api.VDSEvent;
import com.informatica.vds.api.VDSEventList;

public class SeparatorMarshaller implements VDSMarshaller {
    private SeparatorType sepType;
    /**
     * This array contains the separator bytes in the reverse order.
     * It is used by the algorithm which marshals chunk of data and removes any partial data from it
     */
    private byte[] reversedSeparatorBytes;
    /**
     * This array contains the separator bytes in actual order.
     * It is used by the algorithm which parses a data chunk into individual records. 
     */
    private byte[] separatorBytes;

    // this utility method is used by File & TCP sources
    // TODO - this code is dirty. we need to refactor this. check SeparatorType doc
    public static VDSMarshaller getSeparatorMarshaller(VDSConfiguration ctx) throws Exception {
        boolean custom = ctx.optBoolean(ReaderConstants.SRC_CFG_SEPARATOR_FLAG, false);
        VDSMarshaller marshaller = null;
        if (custom) {
            byte[] sepBytes = ctx.getString(ReaderConstants.SRC_CFG_SEPARATOR).getBytes();
            marshaller = new SeparatorMarshaller(SeparatorType.CUSTOM, sepBytes);
        } else {
            SeparatorType type = SeparatorType.fromInt(ctx.optInt(ReaderConstants.SRC_CFG_SEPARATOR, 0));
            if (type != SeparatorType.RAW) {
                marshaller = new SeparatorMarshaller(type);
            }
        }
        return marshaller;
    }

    public SeparatorMarshaller(SeparatorType type) {
        this(type, type.getByteSequence());
    }

    public SeparatorMarshaller(SeparatorType type, byte[] bytes) {
        this.sepType = type;
        if (bytes != null) {
            // copy custom bytes without reversing
            this.separatorBytes = new byte[bytes.length];
            for (int i = 0; i < bytes.length; i++) {
                separatorBytes[i] = bytes[i];
            }

            // we need to reverse the bytes for the algorithm.
            for (int i = 0; i < bytes.length / 2; i++) {
                byte tmp = bytes[i];
                bytes[i] = bytes[bytes.length - 1 - i];
                bytes[bytes.length - 1 - i] = tmp;
            }
        }
        this.reversedSeparatorBytes = bytes;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.um.binge.reader.Marshaler#marshal(byte[], int)
     */
    @Override
    public int marshal(VDSEvent event) {
        int dataLen = 0;
        VDSEventImpl eventImpl = (VDSEventImpl) event;
        if (sepType.isLengthDelimeter()) {
            dataLen = parseFixedLengthData(eventImpl);
        } else {
            dataLen = parseEndDelimitedData(reversedSeparatorBytes, eventImpl);
        }
        return dataLen;
    }

    /**
     * It parses data which is separated at the end by single or multi byte sequences
     * @param sepBytes
     * @param byteBlock
     * @return
     */
    private int parseEndDelimitedData(byte[] sepBytes, VDSEventImpl event) {
        int msgDataLen = 0;
        int numMatch = 0;
        int totalDataLen = event.getBufferLen();
        ByteBuffer dataBuf = event.getBuffer();
        while (totalDataLen > sepBytes.length && numMatch != sepBytes.length) {
            numMatch = 0;
            for (byte comp : sepBytes) {
                if (dataBuf.get(totalDataLen - 1) != comp) {
                    totalDataLen--;
                    break;
                } else {
                    numMatch++;
                    if (numMatch != sepBytes.length)
                        totalDataLen--;
                }
            }
        }
        if (numMatch == sepBytes.length) {
            msgDataLen = (totalDataLen + sepBytes.length - 1);
        }
        return msgDataLen;
    }

    /**
     * Parse fixed length block data
     * @param byteBlock
     * @return true if the block has records. false - no records.
     */
    private int parseFixedLengthData(VDSEventImpl event) {
        int msgDataLen = 0;
        ByteBuffer dataBuf = event.getBuffer();
        int curPos = dataBuf.position();
        int totalDataLen = event.getBufferLen();
        int nextRecordLen = getRecordLen(dataBuf, curPos);
        while (nextRecordLen > 0) {
            curPos = getNextPosition(curPos, nextRecordLen);
            if (curPos <= totalDataLen) {
                msgDataLen = curPos;
                nextRecordLen = getRecordLen(dataBuf, curPos);
            } else {
                break;
            }
        }
        return msgDataLen;
    }

    /**
     *  Calculate the record length from the current position in byte array.
     * @param arr - byte array
     * @param pos - The position in byte array from which record length should be computed.
     * @return - 0 if the array doesn't have enough bytes
     */
    private int getRecordLen(ByteBuffer arr, int pos) {
        int len = 0;
        switch (sepType) {
            case L1:
                if (pos < arr.capacity())
                    len = arr.get(pos) & 0xff;
                break;
            case L2:
                if (pos + 1 < arr.capacity()) {
                    arr.order(ByteOrder.LITTLE_ENDIAN);
                    len = arr.getShort(pos) & 0xffff;
                }
                break;
            case L4:
                if (pos + 1 < arr.capacity()) {
                    arr.order(ByteOrder.LITTLE_ENDIAN);
                    len = arr.getInt(pos) & 0xffffffff;
                }
                break;
        }
        return len;
    }

    private int getNextPosition(int curPos, int recordLen) {
        int nextPos = 0;
        switch (sepType) {
            case L1:
                nextPos = curPos + 1 + recordLen;
                break;
            case L2:
                nextPos = curPos + 2 + recordLen;
                break;
            case L4:
                nextPos = curPos + 4 + recordLen;
                break;
        }
        return nextPos;
    }

    @Override
    public void marshalByteBlockIntoIndividualRecords(VDSEvent src, VDSEventList outEvents) throws Exception {
        if (sepType.equals(SeparatorType.CUSTOM)) {
            parseEndDelimitedDataIntoIndividualRecords(outEvents, (VDSEventImpl) src, separatorBytes);
        } else if (sepType.isLengthDelimeter()) {
            parseFixedLengthDataInIndividualRecords(outEvents, (VDSEventImpl) src);
        } else {
            parseEndDelimitedDataIntoIndividualRecords(outEvents, (VDSEventImpl) src, sepType.getByteSequence());
        }
    }

    private void parseEndDelimitedDataIntoIndividualRecords(VDSEventList outEvents, VDSEventImpl srcEvent,
            byte[] sepBytes2) throws Exception {
        int customSeparatorLength = sepBytes2.length;
        byte[] splitSrc = null;
        ByteBuffer dataBuf = srcEvent.getBuffer();
        // index of byte from where split starts
        int copyStartIndex = dataBuf.position();
        byte[] src = dataBuf.array();
        copyStartIndex = adjustStartIndexForSTX(copyStartIndex, src[copyStartIndex]);
        int srcLength = srcEvent.getBufferLen();
        if (findSubsequenceStartIndex(copyStartIndex, src, sepBytes2) != -1) {
            // index of byte just before which split ends
            int copyEndIndex = 0;
            for (int i = 0; i < srcLength - customSeparatorLength && copyStartIndex < srcLength; i++) {
                copyEndIndex = findSubsequenceStartIndex(copyStartIndex, src, sepBytes2);
                if (copyEndIndex == -1 || copyEndIndex == srcLength) {
                    // next subsequence not found
                    break;
                }
                // copy all bytes from start to one before end index
                splitSrc = new byte[copyEndIndex - copyStartIndex];
                int k = 0;
                for (int j = copyStartIndex; j < copyEndIndex; j++) {
                    splitSrc[k++] = src[j];
                }
                // TODO: this can be further optimized by using VDSEventListImpl
                outEvents.addEvent(splitSrc, splitSrc.length);
                copyStartIndex = copyEndIndex + customSeparatorLength;
                if(copyStartIndex >= src.length){
                    //reached the end of the src array
                    break;
                }
                copyStartIndex = adjustStartIndexForSTX(copyStartIndex, src[copyStartIndex]);
            }
        } else {
            copyInputToOutput((VDSEventListImpl) outEvents, srcEvent);
        }
    }

    //if the delimiter is STXETX then we need to strip off the first byte which is STX
    private int adjustStartIndexForSTX(int copyStartIndex, byte srcByte) {
        if(this.sepType  ==  SeparatorType.STXETX && srcByte == ReaderConstants.STX_SEPRATOR){
            ++copyStartIndex;
        }
        return copyStartIndex;
    }

    private int findSubsequenceStartIndex(int copyStartIndex, byte[] src, byte[] sepBytesSeq) {
        int subSeqStartIndex = -1;
        for (int i = copyStartIndex; i < src.length;) {
            int j = 0;
            // matched - increment both
            while (i < src.length && j < sepBytesSeq.length && src[i] == sepBytesSeq[j]) {
                i++;
                j++;
            }
            // if j has reached the end of byteSeq that means we had found a
            // match
            if (j == sepBytesSeq.length) {
                subSeqStartIndex = i - j;
                break;
            } else if(j == 0){
                //if nothing matched, that means we never went inside the while loop above, so increment i
                //else, dont increment i as that would mean it gets incremented twice, once inside the while and once here.
                i++;
            }
        }
        return subSeqStartIndex;
    }

    private void copyInputToOutput(VDSEventListImpl outEvents, VDSEventImpl srcEvent) throws Exception {
        VDSEventImpl out = outEvents.getEvent(srcEvent.getBlockSize());
        out.setData(srcEvent.getBuffer(), srcEvent.getBufferLen());
        outEvents.addEvent(out);
    }

    private int getSeparatorLength() {
        int separatorLength = 0;
        switch (sepType) {
            case L1:
                separatorLength = 1;
                break;
            case L2:
                separatorLength = 2;
                break;
            case L4:
                separatorLength = 4;
                break;
        }
        return separatorLength;
    }

    private void parseFixedLengthDataInIndividualRecords(VDSEventList outEvents, VDSEventImpl srcEvent)
            throws Exception {
        ByteBuffer buf = srcEvent.getBuffer();
        byte[] src = buf.array();
        int curPos = buf.position();
        int nextRecordLen = getRecordLen(buf, curPos);
        int separatorLength = getSeparatorLength();
        byte[] individualRecord = null;
        int srcLength = srcEvent.getBufferLen();
        while (nextRecordLen > 0) {
            // special handling for the last record
            if (srcLength - (curPos + separatorLength) < nextRecordLen) {
                nextRecordLen = srcLength - (curPos + separatorLength);
            }
            // copy this much length from the src buffer to individual
            // records buffer
            individualRecord = new byte[nextRecordLen];
            System.arraycopy(src, curPos + separatorLength, individualRecord, 0, nextRecordLen);
            outEvents.addEvent(individualRecord, individualRecord.length);
            curPos = getNextPosition(curPos, nextRecordLen);
            if (curPos + separatorLength < srcLength) {
                nextRecordLen = getRecordLen(buf, curPos);
            } else {
                break;
            }
        }
    }
}
