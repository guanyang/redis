package org.gy.framework.redis.support;

public class DiscardShardException extends RuntimeException {

    private static final long serialVersionUID = 8907673892436761336L;

    public DiscardShardException() {
        super();
    }

    public DiscardShardException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public DiscardShardException(String message, Throwable cause) {
        super(message, cause);
    }

    public DiscardShardException(String message) {
        super(message);
    }

    public DiscardShardException(Throwable cause) {
        super(cause);
    }

}
