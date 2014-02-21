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

package com.informatica.surf.sample;

import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsScope;
import java.util.HashMap;

/**
 * TODO: dump these metrics to console on an interval
 * @author Informatica Corp
 */
class MetricsScope implements IMetricsScope {

    public MetricsScope() {
    }

    class Tuple{
        public double value;
        public StandardUnit unit;
        public Tuple(double d, StandardUnit su){
            value = d;
            unit = su;
        }
    }
    private final HashMap <String, Tuple>_metrics = new HashMap<>();
    private final HashMap<String, String> _dimensions = new HashMap<>();
    @Override
    public void addData(String string, double d, StandardUnit su) {
        _metrics.put(string, new Tuple(d, su));
    }

    @Override
    public void addDimension(String name, String value) {
        _dimensions.put(name, value);
    }

    @Override
    public void end() {
        // NOP
    }
    
}
