FROM maven:3.9-eclipse-temurin-21 AS builder
WORKDIR /build
COPY pom.xml .
RUN mvn dependency:go-offline
COPY src/ src/
RUN mvn clean package -DskipTests

FROM eclipse-temurin:21-jre-alpine
RUN apk add --no-cache snappy-dev
WORKDIR /app
COPY --from=builder /build/target/bitmap-filter-service-*.jar app.jar
RUN addgroup -S app && adduser -S app -G app && mkdir -p /data/rocksdb && chown -R app:app /data
USER app
HEALTHCHECK --interval=10s --timeout=3s CMD wget -q -O- http://localhost:8080/health || exit 1
ENV JAVA_OPTS="-Xms2g -Xmx8g -XX:+UseZGC -XX:+ZGenerational"
EXPOSE 50051
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]