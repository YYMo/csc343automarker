import java.sql.*;

public class Assignment2 {
    
    // A connection to the database
    Connection connection;
    
    // Statement to run queries
    Statement sql;
    
    // Prepared Statement
    PreparedStatement ps;
    
    // Resultset for the query
    ResultSet rs;
    
    //CONSTRUCTOR
    Assignment2(){
        try {
            Class.forName("org.postgresql.Driver");
        }
        catch (ClassNotFoundException e) {
            return;
        }
    }
    //Using the input parameters, establish a connection to be used for this session. Returns true if connection is successful
    public boolean connectDB(String URL, String username, String password){
        try {
            connection = DriverManager.getConnection(URL, username, password);
            if (connection !=null) {
                return true;
            }
        }
        catch(SQLException e) {
            return false;
        }
	return true;
    }
    
    //Closes the connection. Returns true if closure was successful
    public boolean disconnectDB(){
        try {
            connection.close();
            return true;
        }
        catch(SQLException se) {
            return false;
        }
    }
        
    public boolean insertCountry (int cid, String name, int height, int population) {
        try {
            int numModRows;
            Statement stmt = null;
            ResultSet rs = null;
            String queryString = null;
                
            stmt = connection.createStatement();
            //first check if country already exists
            queryString = "SELECT cid "
                        + "FROM a2.country "
                        + "WHERE cid = " + cid + ";";
            rs = stmt.executeQuery(queryString);
                
            String tmpCountry;
                
            if (rs.next()) {
                return false;
            }
            else {
                //now insert country
                queryString = "INSERT INTO a2.country "
                            + "VALUES(" + cid + ",'" + name + "', " + height + "," + population + ")";
                numModRows = stmt.executeUpdate(queryString);
                if (numModRows == 1) {
                    rs.close();
                    return true;
                }
                else {
                    return false;
                }
            }
        }
        catch(SQLException se) {
            return false;
        }
    }
        
    public int getCountriesNextToOceanCount(int oid) {
        int countryCount = -1;
        try {
            Statement stmt = null;
            ResultSet rs = null;
            
            String queryString;
            
            stmt = connection.createStatement();
            
            queryString = "SELECT count(cid) "
                        + "FROM a2.oceanAccess "
                        + "WHERE oid = " + oid + ";";
            
            rs = stmt.executeQuery(queryString);
            
            if (rs.next()) {
                countryCount = rs.getInt(1);
                rs.close();
                return countryCount;
            }
        }
        catch(SQLException se) {
            return -1;
        }
        
        return countryCount;
    }
        
    public String getOceanInfo(int oid){
        String ocean = "";
        Statement stmt = null;
        ResultSet rs = null;
        String queryString = "";
        try {
            stmt = connection.createStatement();
                
            queryString = "SELECT oid, oname, depth "
                        + "FROM a2.ocean "
                        + "WHERE oid = " + oid + ";";
                
            rs = stmt.executeQuery(queryString);
                
            if (rs.next()) {
                ocean = ocean + rs.getString(1) + ":" + rs.getString(2) + ":" + rs.getString(3);
                rs.close();
                return ocean;
            }
            else {
                //Ocean doesn't exist
                return ocean;
                }
            }
            catch(SQLException se) {
                return "";
            }
        }
        
        public boolean chgHDI(int cid, int year, float newHDI){
            try {
                int numModRows;
                Statement stmt = null;
                String updateString;
                
                stmt = connection.createStatement();
                updateString = "UPDATE a2.hdi "
                             + "SET hdi_score = " + newHDI + ""
                             + "WHERE cid = " + cid + "AND year = " + year + ";";
                numModRows = stmt.executeUpdate(updateString);
                
                if (numModRows == 1) {
                    stmt.close();
                }
            }
            catch(SQLException se) {
                return false;
            }
            return true;
        }
        
        public boolean deleteNeighbour(int c1id, int c2id){
            try {
                int numModRows;
                int numModRows2;
                Statement stmt = null;
                String deleteString;
                String deleteString2;
                
                stmt = connection.createStatement();
                deleteString = "DELETE FROM a2.neighbour where country = " + c1id + " AND neighbor = " + c2id;
                numModRows = stmt.executeUpdate(deleteString);
                
                if (numModRows > 0) {
                    deleteString2 = "DELETE FROM a2.neighbour where country = " + c2id + " AND neighbor = " + c1id;
                    numModRows2 = stmt.executeUpdate(deleteString2);
                    
                    if (numModRows2 > 0) {
                        stmt.close();
                    }
                }
                
            }
            catch(SQLException se) {
                return false;
            }
            return true;
        }
        
        public String listCountryLanguages(int cid){
            String languages = "";
            Statement stmt = null;
            ResultSet rs = null;
            String queryString;
            float population;
            
            try {
                stmt = connection.createStatement();
                queryString = "SELECT lid, lname, lpercentage, population "
                            + "FROM a2.country INNER JOIN a2.language ON a2.country.cid = a2.language.cid "
                            + "WHERE country.cid = " + cid + "ORDER BY population";
                rs = stmt.executeQuery(queryString);
                
                int index;
                for (index=0; rs.next(); index++) {
                    if (index > 0)
                        languages += "#";
                    languages += rs.getInt(1);
                    languages += ":" + rs.getString(2);
                    population = rs.getInt(4)*rs.getFloat(3);
                    languages += ":" + population;
                }
                return languages;
            }
            catch(SQLException se) {
                return languages; 
            }
        }
        
        public boolean updateHeight(int cid, int decrH){
            try {
                int numModRows;
                Statement stmt = null;
                String updateString;
                
                stmt = connection.createStatement();
                
                updateString = "UPDATE a2.country "
                             + "SET height = (height-" + decrH + ") "
                             + "WHERE cid = " + cid;
                numModRows = stmt.executeUpdate(updateString);
                
                if (numModRows > 0) {
                    return true;
                }
            }
            catch(SQLException se) {
                return false;
            }
            return true;
        }
        
        public boolean updateDB(){
            try {
                Statement stmt = null;
                ResultSet rs = null;

                int numModRows = 0;
                String queryString = "";
                String queryString2 = "";
		String queryString3 = "";
                
                stmt = connection.createStatement();
                
                queryString = "CREATE TABLE a2.mostPopulousCountries(cid INTEGER not NULL, "
                            + "cname VARCHAR(20), "
                            + "PRIMARY KEY (cid))";
                stmt.executeUpdate(queryString);
                

                queryString2 = "INSERT into a2.mostPopulousCountries (cid, cname) SELECT cid, cname from a2.country where population > 100000000 order by population";

                numModRows = stmt.executeUpdate(queryString2);
                if (numModRows > 0) {
                    rs.close();
                    return true;
                }
                else {
                    return false;
                }
            }
            catch(SQLException se) {
                return false;
            }
        }

    
    }

