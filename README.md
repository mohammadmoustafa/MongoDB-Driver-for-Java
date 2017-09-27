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
        <artifactId>mongodb-driver</artifactId>
        <version>3.5.0</version>
    </dependency>
</dependencies>
```
**Note:** Make sure you use the version of MongoDB that works with this driver. You can find the Mongo dependencies [here.](https://mongodb.github.io/mongo-java-driver/)
### Gradle
Add the following to your gradle dependencies:
```
dependencies {
    compile 'org.mongodb:mongodb-driver:3.5.0'
}
```
**Note:** Make sure you use the version of MongoDB that works with this driver. You can find the Mongo dependencies [here.](https://mongodb.github.io/mongo-java-driver/)

### Adding the Driver
Import the provided java file into your project. Once that is done you can start using the driver.
  
## To Do
- [ ] Implement a document update method
- [ ] Add an overloaded method to the constructor to allow specification of the database URI
