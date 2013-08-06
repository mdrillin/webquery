# WebQuery

## Summary

The WebQuery web application can be used for running queries against JDBC sources

This application was written for the purpose of demoing Teiid Dynamic VDB capabilities on OpenShift.  The application can also be used on a standalone local JBoss instance.

## Building the Application

Clone this repo to your system, then build the application war.

$mvn clean install -s settings.xml

This will generate the .war file into the target directory, which you can then drop into your JBoss deployments directory.
Note : The settings.xml file is included, but you will need to modifiy it.  First, install the EAP 6.1 repo locally - then modify settings.xml to reference it - (see Dependencies section)

## Dependencies

The pom.xml provided has dependencies to JBoss EAP 6.1 and Teiid 8.4.1 currently.

 - EAP 6.1 - the maven repo for EAP 6.1 Final can be downloaded from http://www.jboss.org/products/eap.html and installed into your local maven repo
 - Teiid   - the public maven repos for Teiid are located at https://repository.jboss.org/nexus/content/groups/public/org/jboss/teiid/

## Access the application

Once deployed you may access the application in your browser at:

http://[host]/webquery

for example: 

http://localhost:8080/webquery

