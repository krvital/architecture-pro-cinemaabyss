FROM clojure:temurin-21-tools-deps-alpine AS builder

WORKDIR /build
COPY ./ /build
RUN clojure -T:build uber

FROM eclipse-temurin:21-alpine

RUN addgroup -S java && adduser -S java -G java
RUN mkdir -p /app && chown -R java /app
USER java

WORKDIR /app

COPY --from=builder /build/target /app

EXPOSE 8000

ENTRYPOINT ["java", "-jar", "/app/standalone.jar"]
