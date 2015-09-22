package com.dminor.baselib.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Scanner;

public class FileUtil {
	public static String getMD5(File file) {
		FileInputStream fis = null;
		String res = null;
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			fis = new FileInputStream(file);
			byte[] buffer = new byte[8192];
			int length = -1;
			while ((length = fis.read(buffer)) != -1) {
				md.update(buffer, 0, length);
			}
			res = bytesToString(md.digest());
		} catch (Exception ex) {
			try {
				throw ex;
			} catch (Exception e) {
			}
		} finally {
			try {
				if (fis != null) {
					fis.close();
				}
			} catch (IOException ex) {
			}
		}
		return res;
	}

	public static String getMD5(String str) {
		String md5 = null;
		try{
			MessageDigest messageDigest = MessageDigest.getInstance("MD5");
			byte[] inputByteArray = str.getBytes();
			messageDigest.update(inputByteArray, 0, inputByteArray.length);
			md5 = bytesToString(messageDigest.digest());
		}catch(Exception e){
			
		}
		return md5;
	}

	public static String bytesToString(byte[] data) {
		char hexDigits[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
				'a', 'b', 'c', 'd', 'e', 'f' };
		char[] temp = new char[data.length * 2];
		for (int i = 0; i < data.length; i++) {
			byte b = data[i];
			temp[i * 2] = hexDigits[b >>> 4 & 0x0f];
			temp[i * 2 + 1] = hexDigits[b & 0x0f];
		}
		return new String(temp);

	}

	/**
	 * 递归删除目录下的所有文件及子目录下所有文件
	 * 
	 * @param dir
	 *            将要删除的文件目录
	 * @return boolean Returns "true" if all deletions were successful. If a
	 *         deletion fails, the method stops attempting to delete and returns
	 *         "false".
	 */
	static public boolean deleteDir(File dir) {
		if (dir.isDirectory()) {
			String[] children = dir.list();
			for (int i = 0; i < children.length; i++) {
				boolean success = deleteDir(new File(dir, children[i]));
				if (!success) {
					return false;
				}
			}
		}
		// 目录此时为空，可以删除
		return dir.delete();
	}

	public static void copyFile(File sourceFile, File targetFile) {
		BufferedInputStream inBuff = null;
		BufferedOutputStream outBuff = null;
		try {
			inBuff = new BufferedInputStream(new FileInputStream(sourceFile));
			outBuff = new BufferedOutputStream(new FileOutputStream(targetFile));
			byte[] b = new byte[1024 * 8];
			int len;
			while ((len = inBuff.read(b)) != -1) {
				outBuff.write(b, 0, len);
			}
			// 刷新此缓冲的输出流

		} catch (Exception e) {
			throw new Error(e);
		} finally {
			// 关闭流

			try {
				if (inBuff != null)
					inBuff.close();
				if (outBuff != null)
					outBuff.close();
			} catch (IOException e) {
			}

		}
	}

	public static void copyDirectiory(String sourceDir, String targetDir)
			throws IOException {
		(new File(targetDir)).mkdirs();
		File[] file = (new File(sourceDir)).listFiles();

		for (int i = 0; i < file.length; i++) {
			if (file[i].isFile()) {
				File sourceFile = file[i];
				File targetFile = new File(
						new File(targetDir).getAbsolutePath() + File.separator
								+ file[i].getName());
				copyFile(sourceFile, targetFile);
			}
			if (file[i].isDirectory()) {
				String dir1 = sourceDir + "/" + file[i].getName();
				String dir2 = targetDir + "/" + file[i].getName();
				copyDirectiory(dir1, dir2);
			}
		}
	}

	public static ArrayList<String> getFileContentLines(String sourceFile,
			String charset) throws IOException {
		BufferedReader r = null;
		try {
			r = new BufferedReader(new InputStreamReader(new FileInputStream(
					sourceFile), charset));

			ArrayList<String> lines = new ArrayList<String>();
			String line;
			while ((line = r.readLine()) != null) {
				lines.add(new String(line.getBytes(charset), charset));
			}
			return lines;
		} finally {
			if (r != null) {
				r.close();
			}
		}
	}

	public static void dumpToFile(File dstFile, Charset charset, String[] lines)
			throws IOException {
		BufferedWriter w = null;
		try {
			w = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
					dstFile), charset));

			for (String line : lines) {
				String l = new String((line + "\n").getBytes(charset), charset);
				w.write(l);
			}
		} finally {
			if (w != null) {
				w.close();
			}
		}
	}

	/**
	 * 获取字符串文件内容
	 * 
	 * @param filePath
	 * @return
	 * @throws IOException
	 */
	public static String getFileContent(String filePath, String charSet)
			throws IOException {
		File f = null;
		InputStreamReader fin = null;
		BufferedReader bufReader = null;
		StringBuffer buffer = new StringBuffer();
		f = new File(filePath);
		char[] readBuf = new char[1024];
		try {
			fin = new InputStreamReader(new FileInputStream(f), charSet);
			bufReader = new BufferedReader(fin);
			while (bufReader.read(readBuf) != -1) {
				buffer.append(readBuf);
			}
		} catch (UnsupportedEncodingException e) {
			throw e;
		} catch (FileNotFoundException e) {
			throw e;
		} catch (IOException e) {
			return null;
		} finally {
			if (fin != null) {
				fin.close();
			}
		}
		return buffer.toString();
	}

	/**
	 * 获取字符串文件内容
	 * 
	 * @param filePath
	 * @return
	 * @throws IOException
	 */
	public static String getFileContent(String filePath) throws IOException {
		return getFileContent(filePath, System.getProperty("file.encoding"));
	}

	/**
	 * 把文本内容写入指定文件中
	 * 
	 * @param filePath
	 *            要写入的文件，该文件或文件所在文件夹不存在会被创建
	 * @param content
	 *            要写入的内容
	 * @throws IOException
	 */
	public static void setFileContent(String filePath, String content)
			throws IOException {
		File f = new File(filePath);
		if (!f.getParentFile().exists()) {
			f.getParentFile().mkdirs();
		}
		FileWriter out = new FileWriter(filePath, false);
		out.write(content);
		out.close();
	}

	/**
	 * 把文本内容写入指定文件中
	 * 
	 * @param filePath
	 *            要写入的文件，该文件或文件所在文件夹不存在会被创建
	 * @param content
	 *            要写入的内容
	 * @param charSet
	 *            指定文件编码
	 * @throws IOException
	 */
	public static void setFileContent(String filePath, String content,
			String charSet) throws IOException {
		OutputStreamWriter out = null;
		try {
			out = new OutputStreamWriter(new FileOutputStream(filePath, false),
					charSet);
			out.write(content);
			out.close();
		} finally {
			out.close();
		}
	}
}
