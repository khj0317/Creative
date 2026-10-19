# 빌드: ./gradlew bootJar 후 docker compose --profile cluster up -d --build
# (Gradle 캐시를 그대로 쓰기 위해 이미지 안에서 빌드하지 않고 산출물 jar 만 복사한다)
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
RUN addgroup -S app && adduser -S app -G app
COPY build/libs/realtime-session-replay-*.jar app.jar
USER app
EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
