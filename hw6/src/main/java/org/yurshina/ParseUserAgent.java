package org.yurshina;

import eu.bitwalker.useragentutils.UserAgent;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;

import java.util.ArrayList;
import java.util.List;

/**
 * Parses user agent.
 *
 * @author Anastasiia_Iurshina
 */
@UDFType(deterministic = false)
public class ParseUserAgent extends UDF {

    public List<String> evaluate(String row) {
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

        List<String> res = new ArrayList<String>();
        res.add(userAgent.getBrowser().getName());
        res.add(userAgent.getOperatingSystem().getName());
        res.add(userAgent.getOperatingSystem().getDeviceType().getName());
        res.add(city);

        return res;
    }
}
