package org.gy.framework.redis.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.util.SafeEncoder;

public abstract class Serializer {

    private static final Logger logger = LoggerFactory.getLogger(Serializer.class);

    public static byte[] encode(Serializable object) {
        ByteArrayOutputStream byteOS = null;
        ObjectOutputStream objectOS = null;
        byte[] bytes = null;
        if (object instanceof Integer) {
            bytes = SafeEncoder.encode((object).toString());
        } else if (object instanceof Long) {
            bytes = SafeEncoder.encode((object).toString());
        }
        if (bytes != null) {
            return bytes;
        }
        try {
            byteOS = new ByteArrayOutputStream();
            objectOS = new ObjectOutputStream(byteOS);
            objectOS.writeObject(object);
            bytes = byteOS.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (objectOS != null) {
                try {
                    objectOS.close();
                } catch (IOException e) {
                    logger.warn(e.getMessage());
                }
            }
            if (byteOS != null) {
                try {
                    byteOS.close();
                } catch (IOException e) {
                    logger.warn(e.getMessage());
                }
            }
        }
        return bytes;
    }

    public static Serializable decode(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        try {
            return Integer.valueOf(SafeEncoder.encode(bytes));
        } catch (NumberFormatException ne) {
            Serializable object = null;
            ObjectInputStream objectIS = null;
            ByteArrayInputStream byteIS = null;
            try {
                byteIS = new ByteArrayInputStream(bytes);
                objectIS = new ObjectInputStream(byteIS);
                object = (Serializable) objectIS.readObject();
            } catch (IOException e) {
                object = SafeEncoder.encode(bytes);
            } catch (ClassNotFoundException e) {
                object = SafeEncoder.encode(bytes);
            } finally {
                if (byteIS != null) {
                    try {
                        byteIS.close();
                    } catch (IOException e) {
                        logger.warn(e.getMessage());
                    }
                }
                if (objectIS != null) {
                    try {
                        objectIS.close();
                    } catch (IOException e) {
                        logger.warn(e.getMessage());
                    }
                }
            }
            return object;
        }

    }
}
