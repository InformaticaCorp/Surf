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

public abstract class ReaderConstants {

    // Various separators
    public static final byte LF_SEPARATOR = 0x0A;
    public static final byte ETX_SEPRATOR = 0x03;
    public static final byte STX_SEPRATOR = 0x02;
    public static final byte CR_SEPARATOR = 0x0D;
    public static final byte NUL_SEPRATOR = 0x00;

    public static final String SRC_CFG_SEPARATOR_FLAG = "sepFlag";
    public static final String SRC_CFG_SEPARATOR = "sepValue";
}
