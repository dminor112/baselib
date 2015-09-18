package com.dminor.baselib.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * 序列化工具字节
 */
public class SerializationUtil {
    /**
     * 序列化
     * 
     * @param Serializable obj
     * @return byte[]对象
     */
    public static byte[] object2Bytes(Serializable obj) {
        if (obj == null) {
            return null;
        }
        ObjectOutputStream oo = null;
        try {
            ByteArrayOutputStream bo = new ByteArrayOutputStream();
            oo = new ObjectOutputStream(bo);
            oo.writeObject(obj);
            bo.close();
            oo.close();
            return bo.toByteArray();
        } catch (FileNotFoundException e) {
            throw new RuntimeException("FileNotFoundException occurred. ", e); 
        } catch (Exception e) {
            throw new RuntimeException("Exception occurred. ", e);
        } finally {
            if (oo != null) {
                try {
                    oo.close();
                } catch (IOException e) {
                    throw new RuntimeException("IOException occurred. ", e);
                }
            }
        }
    }

    /**
     * 序列化
     * 
     * @param byte[]
     * @return Object对象
     */
    public static Object bytes2Object(byte[] objBytes) {
        if (objBytes == null || objBytes.length == 0) {
            return null;
        }
        ObjectInputStream oi = null;
        try {
            ByteArrayInputStream bi = new ByteArrayInputStream(objBytes);
            oi = new ObjectInputStream(bi);
            Object obj = oi.readObject();
            bi.close();
            oi.close();
            return obj;
        } catch (FileNotFoundException e) {
            throw new RuntimeException("FileNotFoundException occurred. ", e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("ClassNotFoundException occurred. ", e);
        } catch (Exception e) {
            throw new RuntimeException("Exception occurred. ", e);
        } finally {
            if (oi != null) {
                try {
                    oi.close();
                } catch (IOException e) {
                    throw new RuntimeException("IOException occurred. ", e);
                }
            }
        }
    }

    /**
     * 反序列化
     * 
     * @param filePath 序列化文件路径
     * @return 得到的对象
     */
    public static Object deserialization(String filePath) {
        ObjectInputStream in = null;
        try {
            in = new ObjectInputStream(new FileInputStream(filePath));
            Object o = in.readObject();
            in.close();
            return o;
        } catch (FileNotFoundException e) {
            throw new RuntimeException("FileNotFoundException occurred. ", e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("ClassNotFoundException occurred. ", e);
        } catch (IOException e) {
            throw new RuntimeException("IOException occurred. ", e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    throw new RuntimeException("IOException occurred. ", e);
                }
            }
        }
    }

    /**
     * 序列化
     * 
     * @param filePath 序列化文件路径
     * @param obj 序列化的对象
     * @return
     */
    public static void serialization(String filePath, Object obj) {
        ObjectOutputStream out = null;
        try {
            out = new ObjectOutputStream(new FileOutputStream(filePath));
            out.writeObject(obj);
            out.close();
        } catch (FileNotFoundException e) {
            throw new RuntimeException("FileNotFoundException occurred. ", e);
        } catch (IOException e) {
            throw new RuntimeException("IOException occurred. ", e);
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                    throw new RuntimeException("IOException occurred. ", e);
                }
            }
        }
    }
}
