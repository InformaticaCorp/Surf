package com.informatica.surf.transforms.grok;

import com.informatica.vds.api.VDSConfiguration;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

/**
 * Created by jerry on 02/04/14.
 */
public class GrokTransformTest {
    @org.junit.Test
    public void testApply() throws Exception {
        GrokTransform g = new GrokTransform();
        String input = "141.243.1.172 - - [13/Mar/2014:00:06:13 -0700] \"GET /Software.html HTTP/1.0\" 200 1497";
        String expectedStr ="  { " +
          "\"COMMONAPACHELOG\": \"141.243.1.172 - - [13/Mar/2014:00:06:13 -0700] \"GET /Software.html HTTP/1.0\" 200 1497\", " +
          "\"HOUR\": 0, " +
          "\"INT\": -700, " +
          "\"MINUTE\": 6, " +
          "\"MONTH\": \"Mar\", " +
          "\"MONTHDAY\": 13, " +
          "\"SECOND\": 13, " +
          "\"TIME\": \"00:06:13\", " +
          "\"YEAR\": 2014, " +
          "\"auth\": \"-\", " +
          "\"bytes\": 1497, " +
          "\"clientip\": \"141.243.1.172\", " +
          "\"httpversion\": \"1.0\", " +
          "\"ident\": \"-\", " +
          "\"request\": \"/Software.html\", " +
          "\"response\": 200, " +
          "\"timestamp\": \"13/Mar/2014:00:06:13 -0700\", " +
          "\"verb\": \"GET\" " +
        "}";
        final byte[]arr = input.getBytes();
        VDSConfiguration ctx = mock(VDSConfiguration.class);
        when(ctx.optString("pattern", "%{COMMONAPACHELOG}")).thenReturn("%{COMMONAPACHELOG}");
        g.open(ctx);
        String output = g.convertToJSON(input);
        JSONObject json = (JSONObject)JSONValue.parse(output);
        assertEquals(0, json.get("HOUR"));
        assertEquals(-700, json.get("INT"));
        assertEquals(6, json.get("MINUTE"));
        assertEquals("Mar", json.get("MONTH"));
        assertEquals(13, json.get("MONTHDAY"));
        assertEquals(13, json.get("SECOND"));
        assertEquals("00:06:13", json.get("TIME"));
        assertEquals(2014, json.get("YEAR"));
        assertEquals("-", json.get("auth"));
        assertEquals(1497, json.get("bytes"));
        assertEquals("141.243.1.172", json.get("clientip"));
        assertEquals("1.0", json.get("httpversion"));
        assertEquals("-", json.get("ident"));
        assertEquals("/Software.html", json.get("request"));
        assertEquals(200, json.get("response"));
        assertEquals("13/Mar/2014:00:06:13 -0700", json.get("timestamp"));
        assertEquals("GET", json.get("verb"));

    }
}
