Please run the DBConnectionTest.jar as follows:

1. The deployment structure is as follows:
    conf/
        (log4j.properties)
    lib/
        (jar files)
    DBConnectionTest.jar

2. Run the DBConnectionTest.jar with the following command.
   a. Include all jar files in the classpath
   b. Pass the necessary command line arguments

   java -cp ..\DBConnectionTest;..\DBConnectionTest\*;
   ..\DBConnectionTest\lib\*;
   ..\DBConnectionTest\conf
   com.nextlabs.TestConnection checktablesexist $DBDriver $DBURLState $DBUsernameState $DBPasswordState JMS_PROFILES



--------------------------------------------------------