package com.wuzhiwei.bigdata.api;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.*;
import java.security.spec.PKCS8EncodedKeySpec;
/**
 * @author guangjie.Liao
 * @Title: RSAUtils
 * @ProjectName crm-parent
 * @Description: TODO
 * @date 2019/7/2311:48
 */
public class RSAUtils {
    private static Logger logger = LoggerFactory.getLogger(RSAUtils.class);

    private final static String PRIVATE_KEY ="你的私钥";

    public static void main(String[] args) throws Exception {
        System.out.println(sign("test123"));
    }
    public static String sign(String param) {
        try {
            //获取privatekey
            byte[] privateKeyByte = new Base64().decode(PRIVATE_KEY);
            KeyFactory keyfactory = KeyFactory.getInstance("RSA");
            PKCS8EncodedKeySpec encoderule = new PKCS8EncodedKeySpec(privateKeyByte);
            PrivateKey key = keyfactory.generatePrivate(encoderule);

            //用私钥给入参加签
            Signature sign = Signature.getInstance("SHA1withRSA");
            sign.initSign(key);
            logger.info("签名字符串【"+param+"】");
            sign.update(param.getBytes("utf-8"));
            byte[] signature = sign.sign();
            //将签名的入参转换成16进制字符串
            return Base64.encodeBase64String(signature);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
