package com.pcy.kafkastream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * @author pengchenyu
 * @date 2020/6/30 16:25
 */

public class LogProcessor implements Processor<byte[],byte[]> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = context;
    }

    @Override
    public void process(byte[] dummy, byte[] line) {
        String input = new String(line);
        // 根据前缀过滤日志信息，提取后面的内容
        if(input.contains("PRODUCT_RATING_PREFIX:")){
            System.out.println("product rating coming!!!!" + input);
            input = input.split("PRODUCT_RATING_PREFIX:")[1].trim();
            context.forward("logProcessor".getBytes(), input.getBytes());
        }

    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}

