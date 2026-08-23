# Silievo Java 网关 - 生产镜像（基于已打包的 jar）
# 用法: 先 mvn clean package -DskipTests，或直接复用服务器上的
#       silievo-api-gateway-service-1.0-SNAPSHOT.jar
# 构建: docker compose -f ../docker-deploy/docker-compose.yml build gateway

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# 复制预构建 jar（请把 jar 放到本目录，或改路径）
COPY silievo-api-gateway-service-1.0-SNAPSHOT.jar /app/app.jar

# 内存硬上限由 compose 的 mem_limit 保证（1g）；
# 这里 JVM 自身也设 -Xmx1024m 双保险
ENV JAVA_OPTS="-Xms512m -Xmx1024m -XX:+UseG1GC -XX:+HeapDumpOnOutOfMemoryError"
ENV SPRING_PROFILES_ACTIVE=prod-cn

EXPOSE 3002
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar --spring.profiles.active=$SPRING_PROFILES_ACTIVE"]
