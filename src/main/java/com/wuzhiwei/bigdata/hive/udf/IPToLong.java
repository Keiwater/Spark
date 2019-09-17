package com.wuzhiwei.bigdata.hive.udf;

import org.apache.hadoop.hive.ql.udf.UDFAcos;

public class IPToLong extends UDFAcos {

    public Long evaluate(String ip){

        String[] fragment = ip.split("[.]");

        Long ipNum = 0L;
        for(int i = 0 ;i<fragment.length; i++){
            ipNum = Long.parseLong(fragment[i]) | ipNum << 8L;
        }

        return ipNum;
    }
}
