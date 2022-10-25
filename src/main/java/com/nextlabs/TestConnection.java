package com.nextlabs;

import com.bluejungle.framework.crypt.IDecryptor;
import com.bluejungle.framework.crypt.ReversibleEncryptor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.naming.Context;
import javax.naming.directory.SearchControls;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;

public class TestConnection {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final IDecryptor DECRYPTOR = new ReversibleEncryptor();

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Invalid number of arguments");
            System.exit(1);
        }
        String target = args[0];
        switch (target) {
            case "testDB":
                if (args.length < 5) {
                    LOGGER.error("Not enough arguments for {}", target);
                    LOGGER.error(Arrays.toString(args));
                    System.exit(1);
                }
                if (testDB(args[1], args[2], args[3], args[4])) {
                    System.exit(0);
                } else {
                    System.exit(1);
                }
                break;
            case "testAD":
                if (args.length < 6) {
                    LOGGER.error("Not enough arguments for {}", target);
                    LOGGER.error(Arrays.toString(args));
                    System.exit(1);
                }
                if (testLDAP(args[1], args[2], args[3], args[4], args[5])) {
                    System.exit(0);
                } else {
                    System.exit(2);
                }
                break;
            case "dropTables":
                if (args.length < 6) {
                    LOGGER.error("Not enough arguments for {}", target);
                    LOGGER.error(Arrays.toString(args));
                    System.exit(1);
                }
                if (dropTables(args[1], args[2], args[3], args[4], args[5])) {
                    System.exit(0);
                } else {
                    System.exit(1);
                }
                break;
            case "checkTableExistence":
                if (args.length < 7) {
                    LOGGER.error("Not enough arguments for {}", target);
                    LOGGER.error(Arrays.toString(args));
                    System.exit(2);
                }
                if (checkTableExistence(args[1], args[2], args[3], args[4], args[5], args[6])) {
                    System.exit(0);
                } else {
                    System.exit(1);
                }
                break;
            case "createTables":
                if (args.length < 7) {
                    LOGGER.error("Not enough arguments for {}", target);
                    LOGGER.error(Arrays.toString(args));
                    System.exit(1);
                }
                if (createTables(args[1], args[2], args[3], args[4], args[5], args[6])) {
                    System.exit(0);
                } else {
                    System.exit(1);
                }
                break;
            case "checkUserPermissions":
                if (args.length < 6) {
                    LOGGER.error("Not enough arguments for {}", target);
                    LOGGER.error(Arrays.toString(args));
                    System.exit(1);
                }
                if (checkUserPermissions(args[1], args[2], args[3], args[4], args[5])) {
                    System.exit(0);
                } else {
                    System.exit(1);
                }
                break;
            case "selectFromTable":
                if (args.length < 8) {
                    LOGGER.error("Not enough arguments for {}", target);
                    LOGGER.error(Arrays.toString(args));
                    System.exit(1);
                }
                if (selectFromTable(args[1], args[2], args[3], args[4], args[5], args[6], args[7])) {
                    System.exit(0);
                } else {
                    System.exit(1);
                }
                break;
            case "insertIntoTable":
                if (args.length < 7) {
                    LOGGER.error("Not enough arguments for {}", target);
                    LOGGER.error(Arrays.toString(args));
                    System.exit(1);
                }
                if (insertIntoTable(args[1], args[2], args[3], args[4], args[5], args[6], args[7])) {
                    System.exit(0);
                } else {
                    System.exit(1);
                }
                break;
            case "deleteFromTable":
                if (args.length < 6) {
                    LOGGER.error("Not enough arguments for {}", target);
                    LOGGER.error(Arrays.toString(args));
                    System.exit(1);
                }
                if (deleteFromTable(args[1], args[2], args[3], args[4], args[5])) {
                    System.exit(0);
                } else {
                    System.exit(1);
                }
                break;
            case "writeDBProperties":
                if (args.length < 6) {
                    LOGGER.error("Not enough arguments for {}", target);
                    LOGGER.error(Arrays.toString(args));
                    System.exit(1);
                }
                if (writeDBProperties(args[1], args[2], args[3], args[4], args[5])) {
                    System.exit(0);
                } else {
                    System.exit(1);
                }
                break;
            case "selectQuery":
                if (args.length < 9) {
                    LOGGER.error("Not enough arguments for {}", target);
                    LOGGER.error(Arrays.toString(args));
                    System.exit(1);
                }
                if (selectQuery(args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8])) {
                    System.exit(0);
                } else {
                    System.exit(1);
                }
                break;
            case "isInfoPresent":
                if (args.length < 7) {
                    LOGGER.error("Not enough arguments for {}", target);
                    LOGGER.error(Arrays.toString(args));
                    System.exit(1);
                }
                if (isInfoPresent(args[1], args[2], args[3], args[4], args[5], args[6])) {
                    System.exit(0);
                } else {
                    System.exit(1);
                }
                break;
            case "update31Rules" :
                if (args.length < 5) {
                    LOGGER.error("Not enough arguments for {}", target);
                    LOGGER.error(Arrays.toString(args));
                    System.exit(1);
                }
                if (update31Rules(args[1], args[2], args[3], args[4])) {
                    System.exit(0);
                } else {
                    System.exit(1);
                }
                break;
            default:
                LOGGER.error("Incorrect method name. no such method " + target);
        }
    }

    /**
     * Checks if the user has permissions to create tables in the database.
     *
     * @param driver   the driver used to connect to the database
     * @param DB_URL   the driver used to connect to the database
     * @param username the driver used to connect to the database
     * @param password the driver used to connect to the database
     * @param sqlFile  the driver used to connect to the database
     * @return true if the user has permissions to create tables, false otherwise
     */
    private static boolean checkUserPermissions(
            String driver, String DB_URL, String username, String password, String sqlFile) {

        Connection connection = null;

        try {

            LOGGER.debug("Trying to connect to the database...");
            Class.forName(driver);
            connection = DriverManager.getConnection(DB_URL, username, password);
            if (connection == null) {
                return false;
            }
            LOGGER.debug("Database connected successfully.");

            FileReader fileReader = new FileReader(new File(sqlFile));
            BufferedReader br = new BufferedReader(fileReader);

            String line;
            StringBuilder sb = new StringBuilder();

            while ((line = br.readLine()) != null) {
                sb.append(" ");
                sb.append(line);
            }

            br.close();
            fileReader.close();

            Statement stmt = connection.createStatement();
            String[] sql_Stmt = sb.toString().split(";");

            ResultSet rs = stmt.executeQuery(sql_Stmt[0]);
            int result = 0;

            if (rs.next()) {
                result = rs.getInt("RESULT");
            }

            if (result == 1) {
                LOGGER.debug("User {} has the permissions to create the tables", username);
                return true;
            } else {
                LOGGER.error("User {} does not the permissions to create the tables", username);
                return false;
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return false;
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /**
     * This method is used to write the database properties to properties file.
     *
     * @param outPath  the output file path
     * @param driver   the driver used to connect to the database
     * @param DB_URL   the database URL used to connect to the database
     * @param username the username used to connect to the database
     * @param password the password used to connect to the database
     * @return true, if values were successfully written to the file, false otherwise
     */
    private static boolean writeDBProperties(
            String outPath, String driver, String DB_URL, String username, String password) {
        try {
            Properties properties = new Properties();
            properties.setProperty("driver", driver);
            properties.setProperty("connectionString", DB_URL);
            properties.setProperty("username", username);
            properties.setProperty("password", password);

            File file = new File(outPath);
            FileOutputStream fileOut = new FileOutputStream(file);
            properties.store(fileOut, "DB Properties");
            fileOut.close();
            return true;
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            return false;
        }
    }

    /**
     * This method is used to test database connection.
     *
     * @param driver   the driver used to connect to the database
     * @param DB_URL   the url to connect to the database
     * @param user     the username used to connect to database.
     * @param password the password to connect to the database
     * @return true, if the database could be connected to, and false otherwise
     */
    private static boolean testDB(
            String driver, String DB_URL, String user, String password) {

        Connection connection;

        try {
            LOGGER.debug("Trying to connect to the database...");
            Class.forName(driver);
            connection = DriverManager.getConnection(DB_URL, user, password);
            if (connection == null) {
                return false;
            }
            LOGGER.debug("Database connected successfully.");

            connection.close();
            return true;
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return false;
        }
    }

    /**
     * This method is used to check if a table already exist in a DB.
     *
     * @param driver    the driver-name used to connect to the database.
     * @param DB_URL    the url used to connect to the database.
     * @param username  the username used to connect to the database.
     * @param password  the password used to connect to the database.
     * @param tableName the table-name to be checked.
     * @return true if the table exists, false otherwise
     */
    private static boolean checkTableExistence(
            String driver, String DB_URL, String username, String password, String tableName, String passwordType) {

        Connection connection = null;

        try {
            LOGGER.debug("Trying to connect to the database...");
            Class.forName(driver);

            if (passwordType.trim().equalsIgnoreCase("ENC")) {
                password = DECRYPTOR.decrypt(password);
            }

            connection = DriverManager.getConnection(DB_URL, username, password);
            if (connection == null) {
                return false;
            }
            LOGGER.debug("Database connected successfully.");

            DatabaseMetaData meta = connection.getMetaData();
            ResultSet tables = meta.getTables(null, null, tableName, null);
            if (tables.next()) {
                LOGGER.debug(tableName + " exists!");
                return true;
            } else {
                LOGGER.debug(tableName + " does not exist!");
                return false;
            }

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return false;
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /**
     * This method is used to select a field from the table
     *
     * @param driver     the driver to connect to the database
     * @param DB_URL     the URL to connect to the database
     * @param username   the username to connect to the database
     * @param password   the password to connect to the database
     * @param tableName  the table-name to get the information from
     * @param fieldName  the fieldName to be selected in the table
     * @param OutputFile the output file to be written to
     * @return true, if the operation was successful, false otherwise.
     */
    private static boolean selectFromTable(
            String driver,
            String DB_URL,
            String username,
            String password,
            String tableName,
            String fieldName,
            String OutputFile) {

        Connection connection = null;
        Statement stmt;
        ResultSet rs = null;

        try {
            LOGGER.info("Trying to connect to the database..");
            Class.forName(driver);
            connection = DriverManager.getConnection(DB_URL, username, password);
            if (connection == null) {
                return false;
            }
            LOGGER.info("Database connected successfully.");

            String selectStmt = "SELECT ID, " + fieldName + " FROM " + tableName;

            stmt = connection.createStatement();

            rs = stmt.executeQuery(selectStmt);

            List<String> lines = new ArrayList<>();
            while (rs.next()) {
                long id = rs.getLong("ID");
                String value = rs.getString(fieldName);
                lines.add(id + " " + value);
            }

            Path path = Paths.get(OutputFile);
            Files.write(path, lines, Charset.forName("UTF-8"));

            LOGGER.debug("Successfully written to file " + path);

            return lines.size() > 0;

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return false;
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    private static boolean selectQuery(
            String driver,
            String DB_URL,
            String user,
            String password,
            String passwordType,
            String fieldName,
            String sqlFile,
            String outFile) {

        Connection connection = null;
        Statement stmt;
        BufferedReader br = null;
        try {
            LOGGER.info("Trying to connect to the database...");
            Class.forName(driver);

            if (passwordType.equalsIgnoreCase("ENC")) {
                password = DECRYPTOR.decrypt(password);
            }

            connection = DriverManager.getConnection(DB_URL, user, password);
            if (connection == null) {
                return false;
            }
            LOGGER.info("Database connected successfully...");

            stmt = connection.createStatement();

            br = new BufferedReader(new FileReader(sqlFile));

            String currentLine;
            int count = 0;
            List<String> fields = new ArrayList<>();
            StringBuilder sqlStatement = new StringBuilder();

            while ((currentLine = br.readLine()) != null) {
                if (!currentLine.trim().equalsIgnoreCase("-EOL-")) {
                    sqlStatement.append(" ");
                    sqlStatement.append(currentLine);
                } else {
                    LOGGER.debug("Trying to execute " + sqlStatement);
                    if (!sqlStatement.toString().equals("")) {
                        ResultSet rs = stmt.executeQuery(sqlStatement.toString());
                        while (rs.next()) {

                            LOGGER.debug("Found a record for the last executed query");
                            count++;
                            String field = rs.getString(fieldName);
                            LOGGER.debug(fieldName + " = " + field);
                            fields.add(field);
                        }
                        rs.close();
                    }
                    sqlStatement = new StringBuilder();
                }
            }

            if (count > 0) {
                Path file = Paths.get(outFile);
                LOGGER.debug("Trying to write to file {}", file);
                Files.write(file, fields, Charset.forName("UTF-8"));
            } else {
                LOGGER.debug("Found nothing to write to {}", outFile);
            }
            return true;
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return false;
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
            try {
                if (connection != null) connection.close();
            } catch (SQLException se) {
                LOGGER.error(se.getMessage(), se);
            }
        }
    }

    private static boolean isInfoPresent(
            String driver,
            String DB_URL,
            String user,
            String password,
            String passwordType,
            String sqlFile
    ) {

        Connection connection = null;
        Statement stmt;
        BufferedReader br = null;

        try {
            LOGGER.info("Trying to connect to the database...");
            Class.forName(driver);

            if (passwordType.equalsIgnoreCase("ENC")) {
                password = DECRYPTOR.decrypt(password);
            }

            connection = DriverManager.getConnection(DB_URL, user, password);
            if (connection == null) {
                return false;
            }
            LOGGER.info("Database connected successfully...");

            stmt = connection.createStatement();

            br = new BufferedReader(new FileReader(sqlFile));

            String currentLine;
            int count = 0;
            StringBuilder sqlStatement = new StringBuilder();

            while ((currentLine = br.readLine()) != null) {
                if (!currentLine.trim().equalsIgnoreCase("-EOL-")) {
                    sqlStatement.append(" ");
                    sqlStatement.append(currentLine);
                } else {
                    LOGGER.debug("Trying to execute " + sqlStatement);
                    if (!sqlStatement.toString().equals("")) {
                        ResultSet rs = stmt.executeQuery(sqlStatement.toString());
                        while (rs.next()) {
                            LOGGER.debug("Found a record for the last executed query ");
                            count++;
                        }
                        rs.close();
                    }
                    sqlStatement = new StringBuilder();
                }
            }

            if (count > 0) {
                return true;
            } else {
                LOGGER.debug("No records were found!");
                return false;
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return false;
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
            try {
                if (connection != null) connection.close();
            } catch (SQLException se) {
                LOGGER.error(se.getMessage(), se);
            }
        }
    }
    
    /**
     * This method is used to update rules created in v3.1 to v3.5
     * @param driver      the driver name used to connect to the database
     * @param DB_URL      the URL used to connect to the database
     * @param username    the username used to connect to the database
     * @param encPassword the (encrypted) password used to connect to the database
     * @return true, if the operation was a success, false otherwise.
     */
    private static boolean update31Rules(
    		String driver,
            String DB_URL,
            String username,
            String encPassword) {
    	
        Connection connection = null;
        
        try {
            LOGGER.info("Trying to connect to the database..");
            Class.forName(driver);
            String password = DECRYPTOR.decrypt(encPassword);
            connection = DriverManager.getConnection(DB_URL, username, password);
            if (connection == null) {
                return false;
            }
            LOGGER.info("Database connected successfully..");
        	
            update31CopyAction(connection);
            update31MoveAction(connection);
            update31RemoveClassificationAction(connection);
            update31ClassificationAction(connection);
            update31RemoveProtectionAction(connection);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return false;
        } finally {
        	if(connection != null) {
	            try {
	                connection.close();
	            } catch (SQLException se) {
	                LOGGER.error(se.getMessage(), se);
	            }
        	}
        }
    	
    	return true;
    }
    
    /**
     * This method is used to update Copy action record which created in v3.1
     * @param connection	the database connection to the database
     */
    private static void update31CopyAction(Connection connection) {
    	LOGGER.info("Updating Copy action created in v3.1..");
    	String query = "SELECT ACTIONS.ID FROM ACTIONS, RULES WHERE ACTIONS.ACTION_PLUGIN_ID = 1512300900000001 AND RULES.DELETED = 0 AND ACTIONS.RULE_ID = RULES.ID";
    	ResultSet resultSet = null;
    	Statement statement = null;
    	List<String> insertSQL = new ArrayList<String>();
    	
    	try {
    		statement = connection.createStatement();
    		resultSet = statement.executeQuery(query);
    		
    		while(resultSet.next()) {
    			long actionParamId = getNextId(connection, SCClassTypes.getSCClassType("ACTION_PARAMS").getSequence());
    			String insert = "INSERT INTO ACTION_PARAMS (ID, ACTION_ID, IDENTIFIER, VALUE) VALUES (" + actionParamId + ", " + resultSet.getLong(1) + ", 'overwrite', 'true')";
    			insertSQL.add(insert);
    		}
    		
    		for(String sql : insertSQL) {
    			statement.executeUpdate(sql);
    		}
    	} catch (Exception err) {
    		LOGGER.error(err);
    	} finally {
            if (resultSet != null) {
	    		try {
                    resultSet.close();
	            } catch (SQLException e) {
	                LOGGER.error(e.getMessage(), e);
	            }
            }
            
            if(statement != null) {
            	try {
            		statement.close();
            	} catch (SQLException e) {
            		LOGGER.error(e.getMessage(), e);
            	}
            }
    	}
    	
    	LOGGER.info("Copy action created in v3.1 updated successfully..");
    }
    
    /**
     * This method is used to update Move action record which created in v3.1
     * @param connection	the database connection to the database
     */
    private static void update31MoveAction(Connection connection) {
    	LOGGER.info("Updating Move action created in v3.1..");
    	String query = "SELECT ACTIONS.ID FROM ACTIONS, RULES WHERE ACTIONS.ACTION_PLUGIN_ID = 1512300900000006 AND RULES.DELETED = 0 AND ACTIONS.RULE_ID = RULES.ID";
    	ResultSet resultSet = null;
    	Statement statement = null;
    	List<String> insertSQL = new ArrayList<String>();
    	
    	try {
    		statement = connection.createStatement();
    		resultSet = statement.executeQuery(query);
    		
    		while(resultSet.next()) {
    			long actionParamId = getNextId(connection, SCClassTypes.getSCClassType("ACTION_PARAMS").getSequence());
    			String insert = "INSERT INTO ACTION_PARAMS (ID, ACTION_ID, IDENTIFIER, VALUE) VALUES (" + actionParamId + ", " + resultSet.getLong(1) + ", 'overwrite', 'true')";
    			insertSQL.add(insert);
    		}
    		
    		for(String sql : insertSQL) {
    			statement.executeUpdate(sql);
    		}
    	} catch (Exception err) {
    		LOGGER.error(err);
    	} finally {
            if (resultSet != null) {
	    		try {
                    resultSet.close();
	            } catch (SQLException e) {
	                LOGGER.error(e.getMessage(), e);
	            }
            }
            
            if(statement != null) {
            	try {
            		statement.close();
            	} catch (SQLException e) {
            		LOGGER.error(e.getMessage(), e);
            	}
            }
    	}
    	
    	LOGGER.info("Move action created in v3.1 updated successfully..");
    }
    
    /**
     * This method is used to update Remove Classification action record which created in v3.1
     * @param connection	the database connection to the database
     */
    private static void update31RemoveClassificationAction(Connection connection) {
    	LOGGER.info("Updating Remove Classification action created in v3.1..");
    	String query = "SELECT ACTIONS.ID FROM ACTIONS, RULES WHERE ACTIONS.ACTION_PLUGIN_ID = 1512300900000008 AND RULES.DELETED = 0 AND ACTIONS.RULE_ID = RULES.ID";
    	ResultSet resultSet = null;
    	Statement statement = null;
    	List<String> insertSQL = new ArrayList<String>();
    	
    	try {
    		statement = connection.createStatement();
    		resultSet = statement.executeQuery(query);
    		
    		while(resultSet.next()) {
    			String insert = "INSERT INTO ACTION_PARAMS (ID, ACTION_ID, IDENTIFIER, VALUE) VALUES (" + (resultSet.getLong(1) - 10000L) + ", " + resultSet.getLong(1) + ", 'edit-encrypted-document', 'false')";
    			insertSQL.add(insert);
    		}
    		
    		for(String sql : insertSQL) {
    			statement.executeUpdate(sql);
    		}
    	} catch (Exception err) {
    		LOGGER.error(err);
    	} finally {
            if (resultSet != null) {
	    		try {
                    resultSet.close();
	            } catch (SQLException e) {
	                LOGGER.error(e.getMessage(), e);
	            }
            }
            
            if(statement != null) {
            	try {
            		statement.close();
            	} catch (SQLException e) {
            		LOGGER.error(e.getMessage(), e);
            	}
            }
    	}
    	
    	LOGGER.info("Remove Classification action created in v3.1 updated successfully..");
    }
    
    /**
     * This method is used to update Classification action record which created in v3.1
     * @param connection	the database connection to the database
     */
    private static void update31ClassificationAction(Connection connection) {
    	LOGGER.info("Updating Classification action created in v3.1..");
    	String query = "SELECT ACTIONS.ID FROM ACTIONS, RULES WHERE ACTIONS.ACTION_PLUGIN_ID = 1512300900000009 AND RULES.DELETED = 0 AND ACTIONS.RULE_ID = RULES.ID";
    	ResultSet resultSet = null;
    	Statement statement = null;
    	List<String> insertSQL = new ArrayList<String>();
    	
    	try {
    		statement = connection.createStatement();
    		resultSet = statement.executeQuery(query);
    		
    		while(resultSet.next()) {
    			String insert = "INSERT INTO ACTION_PARAMS (ID, ACTION_ID, IDENTIFIER, VALUE) VALUES (" + (resultSet.getLong(1) - 10000L) + ", " + resultSet.getLong(1) + ", 'edit-encrypted-document', 'false')";
    			insertSQL.add(insert);
    		}
    		
    		for(String sql : insertSQL) {
    			statement.executeUpdate(sql);
    		}
    	} catch (Exception err) {
    		LOGGER.error(err);
    	} finally {
            if (resultSet != null) {
	    		try {
                    resultSet.close();
	            } catch (SQLException e) {
	                LOGGER.error(e.getMessage(), e);
	            }
            }
            
            if(statement != null) {
            	try {
            		statement.close();
            	} catch (SQLException e) {
            		LOGGER.error(e.getMessage(), e);
            	}
            }
    	}
    	
    	LOGGER.info("Classification action created in v3.1 updated successfully..");
    }
    
    /**
     * This method is used to update Remove Protection action record which created in v3.1
     * @param connection	the database connection to the database
     */
    private static void update31RemoveProtectionAction(Connection connection) {
    	LOGGER.info("Updating Remove Protection action created in v3.1..");
    	String query = "SELECT ACTIONS.ID FROM ACTIONS, RULES WHERE ACTIONS.ACTION_PLUGIN_ID = 1512300900000011 AND RULES.DELETED = 0 AND ACTIONS.RULE_ID = RULES.ID";
    	ResultSet resultSet = null;
    	Statement statement = null;
    	List<String> insertSQL = new ArrayList<String>();
    	
    	try {
    		statement = connection.createStatement();
    		resultSet = statement.executeQuery(query);
    		
    		while(resultSet.next()) {
    			long actionParamId = getNextId(connection, SCClassTypes.getSCClassType("ACTION_PARAMS").getSequence());
    			String insert = "INSERT INTO ACTION_PARAMS (ID, ACTION_ID, IDENTIFIER, VALUE) VALUES (" + actionParamId + ", " + resultSet.getLong(1) + ", 'overwrite', 'true')";
    			insertSQL.add(insert);
    		}
    		
    		for(String sql : insertSQL) {
    			statement.executeUpdate(sql);
    		}
    	} catch (Exception err) {
    		LOGGER.error(err);
    	} finally {
            if (resultSet != null) {
	    		try {
                    resultSet.close();
	            } catch (SQLException e) {
	                LOGGER.error(e.getMessage(), e);
	            }
            }
            
            if(statement != null) {
            	try {
            		statement.close();
            	} catch (SQLException e) {
            		LOGGER.error(e.getMessage(), e);
            	}
            }
    	}
    	
    	LOGGER.info("Remove Protection action created in v3.1 updated successfully..");
    }
    
    /**
     * this method is used to delete a record from the table.
     *
     * @param driver      the driver name used to connect to the database
     * @param DB_URL      the URL used to connect to the database
     * @param username    the username used to connect to the database
     * @param encPassword the (encrypted) password used to connect to the database
     * @param sqlFile     the file containing SQL statements
     * @return true, if the operation was a success, false otherwise.
     */
    private static boolean deleteFromTable(
            String driver,
            String DB_URL,
            String username,
            String encPassword,
            String sqlFile) {

        Connection connection = null;
        Statement stmt;
        BufferedReader br = null;

        try {
            LOGGER.info("Trying to connect to the database..");
            Class.forName(driver);
            String password = DECRYPTOR.decrypt(encPassword);
            connection = DriverManager.getConnection(DB_URL, username, password);
            if (connection == null) {
                return false;
            }
            LOGGER.info("Database connected successfully..");

            stmt = connection.createStatement();

            br = new BufferedReader(new FileReader(sqlFile));

            String currentLine;
            StringBuilder sqlStatement = new StringBuilder();

            while ((currentLine = br.readLine()) != null) {
                if (!currentLine.trim().equalsIgnoreCase("-EOL-")) {
                    sqlStatement.append(" ");
                    sqlStatement.append(currentLine);
                } else {
                    if (!sqlStatement.toString().equals("")) {
                        LOGGER.debug("Trying to execute " + sqlStatement);
                        stmt.executeUpdate(sqlStatement.toString());
                    }
                    sqlStatement = new StringBuilder();
                }
            }
            return true;
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return false;
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
            try {
                if (connection != null) connection.close();
            } catch (SQLException se) {
                LOGGER.error(se.getMessage(), se);
            }
        }
    }

    /**
     * This method is used to create tables in the db using the script file
     *
     * @param driver   the driver used to connect to the database
     * @param DB_URL   the URL to to connect to the database
     * @param username the username to to connect to the database
     * @param password the password to to connect to the database
     * @param sqlFile  the sqlFile containing the sql statements to be executed.
     * @return true if tables were created successfully, false otherwise
     */
    private static boolean createTables(
            String driver, String DB_URL, String username, String password, String passwordType, String sqlFile) {

        Connection connection = null;
        BufferedReader br = null;

        try {
            LOGGER.info("Trying to connect to the database...");
            Class.forName(driver);

            if (passwordType.trim().equalsIgnoreCase("ENC")) {
                password = DECRYPTOR.decrypt(password);
            }
            connection = DriverManager.getConnection(DB_URL, username, password);
            if (connection == null) {
                return false;
            }
            LOGGER.info("Database connected successfully...");

            Statement stmt = connection.createStatement();

            br = new BufferedReader(new FileReader(sqlFile));

            String currentLine;
            StringBuilder sqlStatement = new StringBuilder();
            while ((currentLine = br.readLine()) != null) {
                if (!currentLine.trim().equalsIgnoreCase("-EOL-")) {
                    sqlStatement.append(" ");
                    sqlStatement.append(currentLine);
                } else {
                    if (!sqlStatement.toString().equals("")) {
                        LOGGER.debug("Trying to execute " + sqlStatement);
                        stmt.executeUpdate(sqlStatement.toString());
                    }
                    sqlStatement = new StringBuilder();
                }
            }

            LOGGER.info("Tables created successfully.");
            return true;
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return false;
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException | IOException e) {
                LOGGER.error(e.getMessage(), e);
            }

        }
    }

    /**
     * @param driver             the driver name
     * @param DB_URL             the url to connect to the database
     * @param user               the username
     * @param password           the password
     * @param tableName          the table name to insert into
     * @param dependentTableName the dependent table to insert into
     * @param sqlFile            the sql script file
     * @return true if the operation is a success, false otherwise
     */
    private static boolean insertIntoTable(
            String driver,
            String DB_URL,
            String user,
            String password,
            String tableName,
            String dependentTableName,
            String sqlFile) {

        Connection connection = null;
        long id = 0;
        BufferedReader br = null;

        try {
            LOGGER.info("Connecting to the database...");
            Class.forName(driver);
            connection = DriverManager.getConnection(DB_URL, user, password);
            if (connection == null) {
                return false;
            }
            LOGGER.info("Connection to the database successful...");

            Statement stmt = connection.createStatement();

            br = new BufferedReader(new FileReader(sqlFile));

            String currentLine;
            StringBuilder sqlStatement = new StringBuilder();

            while ((currentLine = br.readLine()) != null) {
                if (!currentLine.trim().equalsIgnoreCase("-EOL-")) {
                    sqlStatement.append(" ");
                    sqlStatement.append(currentLine);
                } else {
                    if (!sqlStatement.toString().equals("")) {
                        id = getNextId(connection, SCClassTypes.getSCClassType(tableName).getSequence());
                        String statement = sqlStatement.toString().replace("id", String.valueOf(id));
                        LOGGER.debug("Trying to execute " + statement);
                        stmt.executeUpdate(statement);

                        // add to dependent table if any
                        if (!dependentTableName.equals("NULL")) {
                            LOGGER.info("Inserting into dependent table " + dependentTableName);
                            if (dependentTableName.equals("DOCUMENT_SIZE_LIMITS")) {
                                if (!insertIntoDocumentSizeLimits(id, connection)) {
                                    return false;
                                }
                            }
                        }
                    }
                    sqlStatement = new StringBuilder();
                }
            }
            return true;
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return false;
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

/*    private static boolean insertIntoDependentTable(
            long id, String dependentTableName, Connection connection) {
        return (dependentTableName.equals("DOCUMENT_SIZE_LIMITS")
                && insertIntoDocumentSizeLimits(id, connection));
    }*/

    private static boolean insertIntoDocumentSizeLimits(long extractor_id, Connection connection) {

        ResultSet rs = null;
        try {
            List<String> insertList = new ArrayList<>();

            String selectStatement = "SELECT ID, DEFAULT_SIZE_LIMIT FROM dbo.[DOCUMENT_EXTRACTORS]";
            String insertTemplate =
                    "INSERT INTO DOCUMENT_SIZE_LIMITS (ID, EXTRACTOR_ID, DOCUMENT_EXTRACTOR_ID, MAX_FILE_SIZE) VALUES (id,"
                            + extractor_id
                            + ", document_extractor_id, max_file_size)";

            Statement stmt = connection.createStatement();

            rs = stmt.executeQuery(selectStatement);

            while (rs.next()) {
                String insertStatement;
                long id = rs.getLong("ID");
                int maxSize = rs.getInt("DEFAULT_SIZE_LIMIT");

                insertStatement = insertTemplate.replace("document_extractor_id", String.valueOf(id));
                insertStatement = insertStatement.replace("max_file_size", String.valueOf(maxSize));
                insertList.add(insertStatement);
            }

            for (String sqlStatement : insertList) {

                long id = getNextId(connection, SCClassTypes.getSCClassType("DOCUMENT_SIZE_LIMITS").getSequence());
                sqlStatement = sqlStatement.replace("id", String.valueOf(id));

                LOGGER.debug("Trying to execute " + sqlStatement);
                stmt.executeUpdate(sqlStatement);
            }

            return true;
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return false;
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /**
     * get the next ID from the sequence
     *
     * @param connection
     * @param sequenceName
     * @return
     */
    private static long getNextId(Connection connection, String sequenceName) {
        int result = 0;
        try {
            Statement stmt = connection.createStatement();

            String sqlStmt = "SELECT NEXT VALUE FOR " + sequenceName + " AS RESULT";
            ResultSet rs = stmt.executeQuery(sqlStmt);

            while (rs.next()) {
                result = rs.getInt("RESULT");
            }

            rs.close();
            return generateId(result);

        } catch (SQLException se) {
            return -1;
        }
    }

    /**
     * This method is used to generate the ID from the value returned by the sequence.
     *
     * @param key
     * @return
     */
    private static long generateId(int key) {

        String PREFIX_FORMAT = "yyMMddHH";
        char PADDING_CHAR = '0';
        byte LENGTH = 0x8;

        String result = toDateString(new Date(), PREFIX_FORMAT);

        result += leftPad(String.valueOf(key), PADDING_CHAR, LENGTH);

        return new Long(result);
    }

    /**
     * This method helps to format the date.
     *
     * @param date
     * @param format
     * @return
     */
    private static String toDateString(Date date, String format) {
        try {
            return (new SimpleDateFormat(format).format(date));
        } catch (Exception err) {
            return null;
        }
    }

    private static String leftPad(String value, char paddingChar, byte length) {
        StringBuilder result = new StringBuilder();
        int padLength = length - value.length();

        while (padLength > 0) {
            result.append(paddingChar);
            padLength--;
        }

        return result.append(value).toString();
    }

    /**
     * This methods helps to drop tables from the database.
     *
     * @param driver   the driver-name used to connect to the database
     * @param DB_URL   the URL to connect to the database
     * @param user     the username to connect to the database
     * @param password the password to connect to the database.
     * @param sqlFile  the sql file containing the sql statements to be executed
     * @return true, if success, false otherwise
     */
    private static boolean dropTables(
            String driver, String DB_URL, String user, String password, String sqlFile) {

        Connection connection = null;
        BufferedReader br = null;
        try {
            LOGGER.info("Trying to connect to the database..");
            Class.forName(driver);
            connection = DriverManager.getConnection(DB_URL, user, password);
            if (connection == null) {
                return false;
            }
            LOGGER.info("Database connected successfully..");

            Statement stmt = connection.createStatement();

            br = new BufferedReader(new FileReader(sqlFile));

            String currentLine;
            StringBuilder sqlStatement = new StringBuilder();
            while ((currentLine = br.readLine()) != null) {
                if (!currentLine.trim().equalsIgnoreCase("-EOL-")) {
                    sqlStatement.append(" ");
                    sqlStatement.append(currentLine);
                } else {
                    if (!sqlStatement.toString().equals("")) {
                        LOGGER.debug("Trying to execute " + sqlStatement);
                        stmt.executeUpdate(sqlStatement.toString());
                    }
                    sqlStatement = new StringBuilder();
                }
            }

            LOGGER.info("Table dropped successfully...");
            return true;
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return false;
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException | IOException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    private static boolean testLDAP(
            String domain, String server, String port, String user, String password) {
        try {
            LOGGER.debug("Testing LDAP connection...");

            Hashtable<String, String> env = new Hashtable<>();
            env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
            env.put(LdapContext.CONTROL_FACTORIES, "com.sun.jndi.ldap.ControlFactory");
            env.put(Context.SECURITY_AUTHENTICATION, "simple");
            env.put(Context.SECURITY_PRINCIPAL, user);
            env.put(Context.SECURITY_CREDENTIALS, password);
            env.put(Context.PROVIDER_URL, "ldap://" + server + ":" + port + "/");
            env.put(Context.STATE_FACTORIES, "PersonStateFactory");
            env.put(Context.OBJECT_FACTORIES, "PersonObjectFactory");

            LdapContext ctx = new InitialLdapContext(env, null);

            SearchControls controls = new SearchControls();
            controls.setSearchScope(SearchControls.SUBTREE_SCOPE);

            ctx.search(domain, "(objectclass=person)", controls);

            LOGGER.debug("LDAP connected successfully.");
            return true;
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage(), ex);
            return false;
        }
    }



/*  public static void writeLog(String line) {

    File fileToCheck = new File("logs/DBConnectionTest.log");

    if (!fileToCheck.exists()) {
      fileToCheck.getParentFile().mkdirs();
    }

    String logEntry = new Timestamp(new java.util.Date().getTime()).toString() + ": " + line + "\n";

    List<String> list = Arrays.asList(logEntry);

    Path file = Paths.get("logs/DBConnectionTest.log");

    try {
      Files.write(
          file,
          list,
          Charset.forName("UTF-8"),
          StandardOpenOption.CREATE,
          StandardOpenOption.APPEND);
    } catch (IOException e) {
      LOGGER.error(e.getMessage(), e);;
    }
    System.out.println(line);
  }*/
}

/**
 * An enum to map DB names to the sequence names.
 *
 * @author pkalra
 */
enum SCClassTypes {
    WATCHERS("WATCHERS", "COMPONENTS_SEQ"),
    RULE_ENGINES("RULE_ENGINES", "COMPONENTS_SEQ"),
    EXTRACTORS("EXTRACTORS", "COMPONENTS_SEQ"),
    JMS_PROFILES("JMS_PROFILES", "JMS_PROFILES_SEQ"),
    SYSTEM_CONFIGS("SYSTEM_CONFIGS", "SYSTEM_CONFIGS_SEQ"),
    DOCUMENT_SIZE_LIMITS("DOCUMENT_SIZE_LIMITS", "DOCUMENT_SIZE_LIMITS_SEQ"),
    ACTION_PARAMS("ACTION_PARAMS", "ACTION_PARAMS_SEQ");

    private String tableName;
    private String sequence;

    SCClassTypes(String tableName, String sequence) {
        this.tableName = tableName;
        this.sequence = sequence;
    }

    public static SCClassTypes getSCClassType(String tableName) {

        if (tableName != null) {
            for (SCClassTypes scClassType : SCClassTypes.values()) {
                if (scClassType.getTableName().equals(tableName)) {
                    return scClassType;
                }
            }
        }

        return null;
    }

    public String getTableName() {
        return tableName;
    }

    public String getSequence() {
        return sequence;
    }
}
