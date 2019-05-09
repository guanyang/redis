package org.gy.framework.redis.response;

public class Response<T> {

    public static final String SUCCESS_CODE    = ResponseCode.SUCCESS.getCode();

    public static final String SUCCESS_MESSAGE = ResponseCode.SUCCESS.getMessage();
    /**
     * 返回码，0表示成功，其他表示失败
     */
    private String             code            = SUCCESS_CODE;
    /**
     * 返回消息
     */
    private String             message         = SUCCESS_MESSAGE;

    /**
     * 返回数据
     */
    private T                  data;

    /**
     * 是否成功判断
     */
    public boolean isSuccess() {
        return SUCCESS_CODE.equals(this.code);
    }

    public void wrapResponse(ResponseCode responseCode,
                             Object... placeholder) {
        this.setCode(responseCode.getCode());
        this.setMessage(responseCode.buildMessage(placeholder));
    }

    /**
     * 获取返回码，0表示成功，其他表示失败
     * 
     * @return code 返回码，0表示成功，其他表示失败
     */
    public String getCode() {
        return code;
    }

    /**
     * 设置返回码，0表示成功，其他表示失败
     * 
     * @param code 返回码，0表示成功，其他表示失败
     */
    public void setCode(String code) {
        this.code = code;
    }

    /**
     * 获取返回消息
     * 
     * @return message 返回消息
     */
    public String getMessage() {
        return message;
    }

    /**
     * 设置返回消息
     * 
     * @param message 返回消息
     */
    public void setMessage(String message) {
        this.message = message;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

}
