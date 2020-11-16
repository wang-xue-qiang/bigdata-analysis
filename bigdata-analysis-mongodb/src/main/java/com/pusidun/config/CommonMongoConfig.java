package com.pusidun.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.mongodb.core.MongoTemplate;

@Configuration
@ConfigurationProperties(prefix = "primary.mongodb") //前缀指向
public class CommonMongoConfig extends AbstractMongoConfig {
    /**
     * MongoTemplate实现
     *
     * @Bean为创建的mongotemplate实例提供一个名称（primarymongotemplate）
     * @Primary 设为默认
     */
    @Primary
    @Override
    public @Bean(name = "primaryMongoTemplate")
    MongoTemplate getMongoTemplate() throws Exception {
        return new MongoTemplate(mongoDbFactory());
    }

}

