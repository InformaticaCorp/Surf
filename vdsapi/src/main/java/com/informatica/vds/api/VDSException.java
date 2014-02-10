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
package com.informatica.vds.api;

/** \file VDSException.java
   \brief VDSException.java
   \author nraveend - Informatica Corporation 
   \version $Id: //UM/Binge/main/dev/bingeapi/src/main/java/com/informatica/vds/api/VDSException.java#1 $ $DateTime: 2013/12/17 00:28:15 $ $Author: nraveend $ Exp $

   All of the documentation and software included in this and any
   other Informatica Corporation Ultra Messaging Releases
   Copyright (C) Informatica Corporation. All rights reserved.
   
   Redistribution and use in source and binary forms, with or without
   modification, are permitted only as covered by the terms of a
   valid software license agreement with Informatica Corporation.
   
   Copyright (C) 2004-2012, Informatica Corporation. All Rights Reserved.

   THE SOFTWARE IS PROVIDED "AS IS" AND INFORMATICA DISCLAIMS ALL WARRANTIES 
   EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION, ANY IMPLIED WARRANTIES OF 
   NON-INFRINGEMENT, MERCHANTABILITY OR FITNESS FOR A PARTICULAR 
   PURPOSE.  INFORMATICA DOES NOT WARRANT THAT USE OF THE SOFTWARE WILL BE 
   UNINTERRUPTED OR ERROR-FREE.  INFORMATICA SHALL NOT, UNDER ANY CIRCUMSTANCES, BE 
   LIABLE TO LICENSEE FOR LOST PROFITS, CONSEQUENTIAL, INCIDENTAL, SPECIAL OR 
   INDIRECT DAMAGES ARISING OUT OF OR RELATED TO THIS AGREEMENT OR THE 
   TRANSACTIONS CONTEMPLATED HEREUNDER, EVEN IF INFORMATICA HAS BEEN APPRISED OF 
   THE LIKELIHOOD OF SUCH DAMAGES.
*/

/**
 * 
 *
 */
public class VDSException extends Exception {

    private static final long serialVersionUID = 1L;
    private static final String[] EMPTY_PARAMS = new String[] {};

    private final VDSErrorCode code;
    private String[] params = EMPTY_PARAMS;

    /**
     * 
     * @param error - Error code
     * @param params - Arguments to error message
     */
    public VDSException(VDSErrorCode error, Object... params) {
        super(error.getMessage(params));
        this.code = error;
        setParams(params);
    }

    /**
     * 
     * @param error - Error code
     * @param t - Exception object
     * @param params - Arguments to error message
     */
    public VDSException(VDSErrorCode error, Throwable t, Object... params) {
        super(error.getMessage(params), t);
        this.code = error;
        setParams(params);
    }

    /**
     * Get the error code 
     * @return - Intger error code
     */
    public int getErrorCodeInt() {
        return code.getCode();
    }

    /**
     * Get the error code
     * @return
     */
    public VDSErrorCode getErrorCode() {
        return code;
    }

    /**
     * Get the error message arguments.
     * @return
     */
    public String[] getParams() {
        return params;
    }

    protected void setParams(Object... params) {
        if (params.length > 0) {
            this.params = new String[params.length];
            for (int i = 0; i < params.length; i++) {
                this.params[i] = params[i] == null ? "" : params[i].toString();
            }
        }
    }
}
