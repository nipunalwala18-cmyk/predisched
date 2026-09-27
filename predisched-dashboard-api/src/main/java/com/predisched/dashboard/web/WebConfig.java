package com.predisched.dashboard.web;

import com.predisched.dashboard.DashboardProperties;
import com.predisched.dashboard.cluster.Cluster;
import com.predisched.dashboard.cluster.GrpcCluster;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/** CORS for the Vite dev server, the admin-key guard, and the gRPC cluster client. */
@Configuration
public class WebConfig implements WebMvcConfigurer {

    private final AdminGuard adminGuard;

    public WebConfig(AdminGuard adminGuard) {
        this.adminGuard = adminGuard;
    }

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/api/**")
                .allowedOrigins("http://localhost:5173")
                .allowedMethods("GET", "POST", "OPTIONS")
                .allowedHeaders("*");
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(adminGuard).addPathPatterns(AdminGuard.PROTECTED);
    }

    /** The cluster over gRPC; a {@link GrpcCluster.Channels} bean replaces TCP (tests). */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean(Cluster.class)
    GrpcCluster cluster(DashboardProperties props,
            org.springframework.beans.factory.ObjectProvider<GrpcCluster.Channels> channels) {
        GrpcCluster.Channels custom = channels.getIfAvailable();
        return custom == null ? new GrpcCluster(props) : new GrpcCluster(props, custom);
    }
}
