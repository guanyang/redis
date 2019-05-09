package org.gy.framework.redis.exception;

public class RedisClientException extends RuntimeException {

    private static final long serialVersionUID = 3318536117529180383L;

    /**
     * 构造异常对象
     * 
     * @param msg
     */
    public RedisClientException(String msg) {
        super(msg);
    }

    /**
     * RedisClientException
     * 
     * @param exception
     */
    public RedisClientException(Throwable exception) {
        super(exception);
    }

    /**
     * RedisClientException
     * 
     * @param mag
     * @param exception
     */
    public RedisClientException(String mag, Exception exception) {
        super(mag, exception);
    }
}
