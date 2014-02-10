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


/**
 * built in separator types
 * TODO - THIS CANNOT BE A ENUM. We should need to refactor this.
 */
public enum SeparatorType {

    LF(0), CRLF(1), STXETX(2), NUL(3), RAW(4), L1(5), L2(6), L4(7), CUSTOM(8);

    private int _type;

    SeparatorType(int type) {
        _type = type;
    }

    public static SeparatorType fromInt(int code) {
        for (SeparatorType type : SeparatorType.values()) {
            if (type.toInt() == code) {
                return type;
            }
        }
        return null;
    }

    public int toInt() {
        return _type;
    }

    public byte[] getByteSequence() {
        byte[] seq = null;
        switch (this) {
            case LF:
                seq = new byte[1];
                seq[0] = ReaderConstants.LF_SEPARATOR;
                break;
            // Multi byte separators should be returned in reverse order.
            case CRLF:
                seq = new byte[2];
                seq[0] = ReaderConstants.CR_SEPARATOR;
                seq[1] = ReaderConstants.LF_SEPARATOR;
                break;
            case STXETX:
                seq = new byte[1];
                seq[0] = ReaderConstants.ETX_SEPRATOR;
                break;
            case NUL:
                seq = new byte[1];
                seq[0] = ReaderConstants.NUL_SEPRATOR;
                break;
        }
        return seq;

    }

    public int length() {
        int length = 0;
        switch (this) {
            case LF:
                length = 1;
                break;
            case CRLF:
                length = 2;
                break;
            case STXETX:
                length = 2;
                break;
            case NUL:
                length = 1;
                break;
        }
        return length;
    }

    public boolean isLengthDelimeter() {
        return (this == L1 || this == L2 || this == L4);
    }

}
