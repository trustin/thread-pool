plugins {
    id("java-library")
}

group = "threadpool"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

dependencies {
    api(platform("org.slf4j:slf4j-bom:2.0.9"))
    api(platform("io.micrometer:micrometer-bom:1.12.3"))
    api("com.google.code.findbugs:jsr305:3.0.2")
    api("org.slf4j:slf4j-api")
    api("io.micrometer:micrometer-core")
    implementation("com.google.guava:guava:32.1.3-jre")

    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.slf4j:slf4j-simple")
    testImplementation("io.micrometer:micrometer-registry-prometheus")
}

tasks.test {
    useJUnitPlatform()
}
