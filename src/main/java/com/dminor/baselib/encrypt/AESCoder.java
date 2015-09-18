package com.dminor.baselib.encrypt;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import org.apache.axis.encoding.Base64;

public class AESCoder extends Coder {
	private static Logger logger = LoggerFactory.getLogger(AESCoder.class);
	public static final String defaultPassword = "fghj56@#,[jk98asw@%vb";
	/**
	 * 加密
	 * 
	 * @param content
	 *            需要加密的内容
	 * @param password
	 *            加密密码
	 * @return
	 */
	public static byte[] encrypt(String content, String password) {
		try {
			KeyGenerator kgen = KeyGenerator.getInstance("AES");
			kgen.init(128, new SecureRandom(password.getBytes()));
			SecretKey secretKey = kgen.generateKey();
			byte[] enCodeFormat = secretKey.getEncoded();
			SecretKeySpec key = new SecretKeySpec(enCodeFormat, "AES");
			Cipher cipher = Cipher.getInstance("AES");// 创建密码器
			byte[] byteContent = content.getBytes("utf-8");
			cipher.init(Cipher.ENCRYPT_MODE, key);// 初始化
			byte[] result = cipher.doFinal(byteContent);
			return result; // 加密
		} catch (NoSuchAlgorithmException e) {
			logger.error("encrypt, {}", e);
		} catch (NoSuchPaddingException e) {
			logger.error("encrypt, {}", e);
		} catch (InvalidKeyException e) {
			logger.error("encrypt, {}", e);
		} catch (UnsupportedEncodingException e) {
			logger.error("encrypt, {}", e);
		} catch (IllegalBlockSizeException e) {
			logger.error("encrypt, {}", e);
		} catch (BadPaddingException e) {
			logger.error("encrypt, {}", e);
		}
		return null;
	}
	
	/**
	 * 加密并转成base64字符，以便网络传输
	 * @param content
	 * @param password
	 * @return
	 */
	public static String encryptToBase64(String content, String password){
		String result = null;
		try{
			result = encryptBASE64(encrypt(content, password));
		}catch (Exception e){
			logger.error("encryptToBase64 error, {}", e);
		}
		return result;
	}

	/**
	 * 解密
	 * 
	 * @param content
	 *            待解密内容
	 * @param password
	 *            解密密钥
	 * @return
	 */
	public static byte[] decrypt(byte[] content, String password) {
		try {
			KeyGenerator kgen = KeyGenerator.getInstance("AES");
			kgen.init(128, new SecureRandom(password.getBytes()));
			SecretKey secretKey = kgen.generateKey();
			byte[] enCodeFormat = secretKey.getEncoded();
			SecretKeySpec key = new SecretKeySpec(enCodeFormat, "AES");
			Cipher cipher = Cipher.getInstance("AES");// 创建密码器
			cipher.init(Cipher.DECRYPT_MODE, key);// 初始化
			byte[] result = cipher.doFinal(content);
			return result; // 加密
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (NoSuchPaddingException e) {
			e.printStackTrace();
		} catch (InvalidKeyException e) {
			e.printStackTrace();
		} catch (IllegalBlockSizeException e) {
			e.printStackTrace();
		} catch (BadPaddingException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 直接对base64解密并采用utf-8转成字符串
	 * @param content
	 * @param password
	 * @return
	 */
	public static String decryptBase64ToUtf8(String content, String password){
		String result = null;
		try{
			result = new String(decrypt(decryptBASE64(content), password), "utf-8");
		}catch(Exception e){
			logger.error("decryptBase64ToUtf8 error, {}", e);
		}
		return result;
	}

	/**
	 * 将二进制转换成16进制
	 * 
	 * @param buf
	 * @return
	 */
	public static String parseByte2HexStr(byte buf[]) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < buf.length; i++) {
			String hex = Integer.toHexString(buf[i] & 0xFF);
			if (hex.length() == 1) {
				hex = '0' + hex;
			}
			sb.append(hex.toUpperCase());
		}
		return sb.toString();
	}

	/**
	 * 将16进制转换为二进制
	 * 
	 * @param hexStr
	 * @return
	 */
	public static byte[] parseHexStr2Byte(String hexStr) {
		if (hexStr.length() < 1)
			return null;
		byte[] result = new byte[hexStr.length() / 2];
		for (int i = 0; i < hexStr.length() / 2; i++) {
			int high = Integer.parseInt(hexStr.substring(i * 2, i * 2 + 1), 16);
			int low = Integer.parseInt(hexStr.substring(i * 2 + 1, i * 2 + 2),
					16);
			result[i] = (byte) (high * 16 + low);
		}
		return result;
	}

	/**
	 * 加密
	 *
	 * @param content
	 *            需要加密的内容
	 * @param password
	 *            加密密码
	 * @return
	 */
	public static byte[] encrypt2(String content, String password) {
		try {
			SecretKeySpec key = new SecretKeySpec(password.getBytes(), "AES");
			Cipher cipher = Cipher.getInstance("AES/ECB/NoPadding");
			byte[] byteContent = content.getBytes("utf-8");
			cipher.init(Cipher.ENCRYPT_MODE, key);// 初始化
			byte[] result = cipher.doFinal(byteContent);
			return result; // 加密
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (NoSuchPaddingException e) {
			e.printStackTrace();
		} catch (InvalidKeyException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IllegalBlockSizeException e) {
			e.printStackTrace();
		} catch (BadPaddingException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args) throws Exception {
		String content = "test阿达67890dfg。、pop";
		String password = defaultPassword;
//		// 加密
//		System.out.println("加密前：" + content);
//		byte[] encryptResult = encrypt(content, password);
//		String tt4 = encryptBASE64(encryptResult);
//		System.out.println(new String(tt4));
//
//		// 解密
//		byte[] decryptResult = decrypt(decryptBASE64(tt4), password);
//		System.out.println("解密后：" + new String(decryptResult));
		
		// 加密
		System.out.println("加密前：" + content);
//		byte[] encryptResult = encrypt(content, password);
		String tt4 = encryptToBase64(content, password);
		System.out.println(new String(tt4));

		// 解密
		String decryptResult = decryptBase64ToUtf8(tt4, password);
		System.out.println("解密后：" + new String(decryptResult));
	}
}
