package com.example.topn.ringbuffer;

import com.lmax.disruptor.ExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FatalExceptionHandler implements ExceptionHandler<Element> {
    private static final Logger logger = LoggerFactory.getLogger(FatalExceptionHandler.class);

    @Override
    public void handleEventException(Throwable ex, long sequence, Element event) {
        logger.error(ex.getMessage(), ex);
    }

    @Override
    public void handleOnStartException(Throwable ex) {

    }

    @Override
    public void handleOnShutdownException(Throwable ex) {

    }
}
