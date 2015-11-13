package org.yurshina;

import eu.bitwalker.useragentutils.UserAgent;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;

import java.util.HashMap;
import java.util.Map;

/**
 * Parses user agent.
 *
 * @author Anastasiia_Iurshina
 */
@UDFType(deterministic = false)
public class ParseUserAgent extends UDF {

    public Map<String, String> evaluate(String row) {
        if (row == null) {
            return null;
        }

        String[] tokens = row.split("\\t");
        if (tokens.length < 8) {
            return null;
        }

        String userAgentString = tokens[4];
        String city = tokens[7];

        UserAgent userAgent = UserAgent.parseUserAgentString(userAgentString);

        Map<String, String> res = new HashMap<String, String>();
        res.put("Browser", userAgent.getBrowser().getName());
        res.put("OS", userAgent.getOperatingSystem().getName());
        res.put("Device", userAgent.getOperatingSystem().getDeviceType().getName());
        res.put("City", city);

        return res;
    }
}
