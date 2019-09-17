package com.wuzhiwei.bigdata.api;


import java.util.TreeMap;

/**
 * @author guangjie.Liao
 * @Title: SignUtils
 * @ProjectName crm-parent
 * @Description: TODO
 * @date 2019/7/2315:21
 */
public class SignUtils {

    private final static String APP_KEY = "f10639e3aa0da9095a1e22192a965bd5";

    private final static String APP_SECRET="NzMxODAzY2M3MGEwMWRhNzI1OGRhNmIxYTI4YTgzMmRmOTIz";


    public static String getSign(TreeMap param,Long ts){
        StringBuffer buffer = new StringBuffer();
        buffer.append(APP_SECRET);
        buffer.append("appkey"+APP_KEY);
        buffer.append("biz_data"+ JSONUtils.beanToJson(param));
        buffer.append("ts"+ts);
        buffer.append(APP_SECRET);
        String sign = null;
        try {
            sign= RSAUtils.sign(buffer.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sign;
    }

    public static String getAppkey(){
        return APP_KEY;
    }
}
