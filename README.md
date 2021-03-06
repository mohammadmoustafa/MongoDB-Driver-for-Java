# MongoDB Driver for Java

This driver was created as a means to simplify the use of MongoDB within a Java project.

## Current Version
The current version of the driver supports MongoDB **v3.5**
## Installation
### Maven
Add the following to your `pom.xml`:
```
<dependencies>
    <dependency>
        <groupId>org.mongodb</groupId>
        <artifactId>mongo-java-driver</artifactId>
        <version>3.5.0</version>
    </dependency>
</dependencies>
```
**Note:** Make sure you use the version of MongoDB that works with this driver. You can find the Mongo dependencies [here.](https://mongodb.github.io/mongo-java-driver/)
### Gradle
Add the following to your gradle dependencies:
```
dependencies {
    compile 'org.mongodb:mongo-java-driver:3.5.0'
}
```
**Note:** Make sure you use the version of MongoDB that works with this driver. You can find the Mongo dependencies [here.](https://mongodb.github.io/mongo-java-driver/)

### Not using Maven or Gradle?
You can download an uber jar for use with your project. Download the jar and include it in your projects build path.

### Adding the Driver
Include the provided jar file in your projects build path.
