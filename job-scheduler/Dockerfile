# syntax=docker/dockerfile:1

################################################################################
# Stage 1: Resolve and download dependencies
################################################################################
FROM eclipse-temurin:25-jdk-jammy as deps

WORKDIR /build

COPY --chmod=0755 mvnw mvnw
COPY .mvn/ .mvn/

# Download dependencies (Cached)
RUN --mount=type=bind,source=pom.xml,target=pom.xml \
    --mount=type=cache,target=/root/.m2 ./mvnw dependency:go-offline -DskipTests

################################################################################
# Stage 2: Build the application
################################################################################
FROM deps as package

WORKDIR /build

COPY ./src src/

# Build and rename the JAR to 'app.jar' for simplicity
RUN --mount=type=bind,source=pom.xml,target=pom.xml \
    --mount=type=cache,target=/root/.m2 \
    ./mvnw package -DskipTests && \
    mv target/job-scheduler-executable.jar target/app.jar

################################################################################
# Stage 3: Runtime Image (Producer/Consumer ready)
################################################################################
FROM eclipse-temurin:25-jre-jammy AS final

# Create non-privileged user
ARG UID=10001
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    appuser

WORKDIR /app
RUN chown ${UID} /app

# --- CRITICAL FIX HERE ---
# We copy from the 'package' stage.
# Since Stage 2 WORKDIR was /build, the jar is at /build/target/app.jar
COPY --from=package /build/target/app.jar /app/app.jar

# Set the default mode (can be overridden at runtime)
ENV APP_MODE="producer"

USER appuser

EXPOSE 8080

ENTRYPOINT [ "java", "-jar", "app.jar" ]