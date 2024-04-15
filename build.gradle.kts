/*
 * User Manual available at https://docs.gradle.org/7.4/userguide/building_java_projects.html
 */

group = "org.alpaka.kafka"
version = System.getenv("VERSION") ?: "1.0.0"

val javaVersion = 11

val artifactoryContext =
    project.properties.getOrDefault("artifactory_context", System.getenv("ARTIFACTORY_CONTEXT")).toString()
val artifactoryUsern =
    project.properties.getOrDefault("artifactory_user", System.getenv("ARTIFACTORY_USER")).toString()
val artifactoryPassword =
    project.properties.getOrDefault("artifactory_password", System.getenv("ARTIFACTORY_PWD")).toString()

plugins {
    kotlin("jvm") version "1.6.21"
    idea // Generates files that are used by IntelliJ IDEA, thus making it possible to open the project from IDEA
    `java-library` // Apply the java-library plugin for API and implementation separation.
    `maven-publish`
    id("io.gitlab.arturbosch.detekt") version "1.20.0"
}

repositories {
    mavenCentral()
}

dependencies {
    val kafkaConnectVersion = "3.7.+"
    val junitVersion = "5.8.2"

    compileOnly(platform("org.jetbrains.kotlin:kotlin-bom")) // Align versions of all Kotlin components
    compileOnly("org.jetbrains.kotlin:kotlin-stdlib-jdk8") // Use the Kotlin JDK 8 standard library.

    implementation("org.apache.kafka:connect-api:$kafkaConnectVersion")
    implementation("org.apache.kafka:connect-json:$kafkaConnectVersion")
    implementation("org.apache.kafka:connect-transforms:$kafkaConnectVersion")

    testImplementation("org.jetbrains.kotlin:kotlin-test-junit")
    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
}

kotlin {
    jvmToolchain {
        (this as JavaToolchainSpec).languageVersion.set(JavaLanguageVersion.of(javaVersion))
    }
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = project.group.toString()
            artifactId = project.name
            version = project.version.toString()

            pom {
                name.set("alpakaby-kafka")
                description.set( project.description)
                url.set("https://github.com/alpakaby/kafka")
                organization {
                    name.set("Alpaka")
                    url.set("https://github.com/alpakaby")
                }
                issueManagement {
                    system.set("GitHub")
                    url.set("https://github.com/alpakaby/kafka/issues")
                }
                licenses {
                    license {
                        name.set( "Apache License 2.0")
                        url.set("https://github.com/alpakaby/kafka/blob/master/LICENSE")
                        distribution.set("repo")
                    }
                }
                developers {
                    developer {
                        name.set("alpakaby")
                    }
                }
                scm {
                    url.set("https://github.com/alpakaby/kafka")
                    connection.set("scm:git:git://github.com/alpakaby/kafka.git")
                    developerConnection.set("scm:git:ssh://git@github.com:alpakaby/kafka.git")
                }
            }

            from(components["java"])
        }
    }
    repositories {
        maven {
            name = "ArtifactoryLocal"
            url = uri(artifactoryContext + "/libs-release-local")
            credentials {
                username = artifactoryUsern
                password = artifactoryPassword
            }
        }
    }
}
