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

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Basic representation of data object in VDS. Provides access to data
 * as it flows through the system.
 *
 */
public interface VDSEvent {

	/**
	 * Returns a name-value pair of the data stored in the body.
	 * @return
	 */
	public Map<String, String> getEventInfo();

	/**
	 * Returns the raw byte array of the data contained in this event.
	 * @return - byte array
	 */
    public ByteBuffer getBuffer();
	
    /**
     * Get the length of the data contained in the byte array
     * @return
     */
    public int getBufferLen();

}
