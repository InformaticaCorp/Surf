package com.informatica.surf;

import com.informatica.um.binge.api.impl.VDSEventListImpl;
import com.lmax.disruptor.EventFactory;

public class SurfEvent {
    private VDSEventListImpl _eventlist;
    public SurfEvent(){

    }

    public VDSEventListImpl getEventlist() {
        return _eventlist;
    }

    public void setEventlist(VDSEventListImpl _eventlist) {
        this._eventlist = _eventlist;
    }

    public static final EventFactory<SurfEvent> EVENT_FACTORY = new EventFactory<SurfEvent>() {
        @Override
        public SurfEvent newInstance() {
            return new SurfEvent();
        }
    };
}
