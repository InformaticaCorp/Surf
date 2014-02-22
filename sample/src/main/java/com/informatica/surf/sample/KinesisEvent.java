package com.informatica.surf.sample;

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
        public KinesisEvent newInstance(){
            return new KinesisEvent();
        }
    };
}
