package org.ohdsi.webapi;

import org.ohdsi.webapi.aspect.SourceDaimonContextAspect;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

@Configuration
@EnableAspectJAutoProxy
public class AspectConfig {

    @Bean
    public SourceDaimonContextAspect sourceDaimonContextAspect() {
        return new SourceDaimonContextAspect();
    }
}
