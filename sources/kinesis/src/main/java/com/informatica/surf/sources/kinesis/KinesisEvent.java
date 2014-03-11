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
package com.informatica.surf.sources.kinesis;

import com.lmax.disruptor.EventFactory;

/**
 * Created by jerry on 22/02/14.
 */
public class KinesisEvent {

    private String _data;

    public String getData() {
        return _data;
    }

    public void setData(String _data) {
        this._data = _data;
    }

    public static final EventFactory<KinesisEvent> EVENT_FACTORY = new EventFactory<KinesisEvent>(){
        @Override
        public KinesisEvent newInstance(){
            return new KinesisEvent();
        }
    };
}
