package com.kafka.consumer.config;

import lombok.Getter;

@Getter
public class ConsumerConfig {
    public static final String KEY_DESERIALIZER = "key.deserializer";
    public  static final String VALUE_DESERIALIZER = "value.deserializer";
    public static final String AUTO_OFFSET_RESET = "auto.offset.reset";
    public static final String GROUP_ID = "group.id";
}
