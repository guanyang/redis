package org.gy.framework.redis.support;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.gy.framework.redis.support.SmartJedis.RW;

import redis.clients.jedis.Builder;
import redis.clients.jedis.Response;

public class DiscartPipeline extends SmartPipeline {

    public DiscartPipeline(SmartJedis smartJedis) {
        super(smartJedis);
    }

    @Override
    protected <T> T execute(Action<T> action,
                            RW rw,
                            String... keys) {
        Type responseResultType = getResponseResultType(action);
        return (T) new DiscardResponse(responseResultType);
    }

    @Override
    protected <T> T executeOnMaster(Action<T> action) {
        Type responseResultType = getResponseResultType(action);
        return (T) new DiscardResponse(responseResultType);
    }

    private Type getResponseResultType(Action action) {
        Type mySuperClass = action.getClass().getGenericSuperclass();
        Type type = ((ParameterizedType) mySuperClass).getActualTypeArguments()[0];
        return ((ParameterizedType) type).getActualTypeArguments()[0];
    }

    /**
     * pipeline丢弃后的响应<br>
     * 对于discard的shard的pipeline的响应，应该直接返回null<br/>
     * 由于只需要在这里使用，外部无需知道，因此使用内部类
     * 
     * @author 17032355
     * @see [相关类/方法]（可选）
     * @since [产品/模块版本] （可选）
     */
    private static class DiscardResponse extends Response<Object> {
        public static final Builder<Object> DISCARTBUILDER = new Builder<Object>() {
                                                               public Object build(Object data) {
                                                                   return null;
                                                               }

                                                               public String toString() {
                                                                   return "discardShardname_builder";
                                                               }
                                                           };
        private Type                        responseResultType;

        public DiscardResponse(Type responseResultType) {
            super(DISCARTBUILDER);
            this.responseResultType = responseResultType;
        }

        @Override
        public Object get() {
            if (responseResultType instanceof Class) {
                if (responseResultType == Long.class) {
                    return 0L;
                } else {
                    return null;
                }
            } else {
                Type type = ((ParameterizedType) responseResultType).getRawType();
                if (type == List.class) {
                    return new ArrayList<Object>();
                } else if (type == Set.class) {
                    return new HashSet<Object>();
                }
            }
            return null;
        }

    }

}
