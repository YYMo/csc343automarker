

import java.sql.*;
import java.io.*;

public class Assignment2 {
    
  // A connection to the database  
    Connection connection;
  
  // Statement to run queries
    Statement sql;

    String query;
  
  // Prepared Statement
    PreparedStatement ps;
  
  // Resultset for the query
    ResultSet rs;
  
  // CONSTRUCTOR
    public  Assignment2() throws ClassNotFoundException{
        try {
            Class.forName("org.postgresql.Driver");
        }
        catch (ClassNotFoundException e) {
            System.out.println("Failed to find the JDBC driver");
        }
    }
  
  // Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
    public boolean connectDB(String URL, String username, String password) throws SQLException{
        try {
            connection = DriverManager.getConnection(URL, username, password);
        }
        catch (SQLException se) {
            return false;
        }
        return true;
        }

        // Closes the connection. Returns true if closure was sucessful
        public boolean disconnectDB(){
        try {
            connection.close();
        }
        catch (SQLException se) {
 //           System.err.println("BBSQL Exception," + "<Message>: " + se.getMessage());
            return false;
        }
        return true;
    }
    
    public boolean insertCountry (int cid, String name, int height, int population){
        try {
            query = "SELECT count(*) as num FROM a2.country WHERE cid=" + String.valueOf(cid);     
            ps = connection.prepareStatement(query);
            rs = ps.executeQuery();
            System.out.println("Query1");
            while (rs.next()) {
                int num = rs.getInt("num");
                System.out.println( num);
                if (num != 0) {
                    return false;
                }
            }

            query = "INSERT INTO a2.country(cid, cname, height, population) VALUES (" + String.valueOf(cid) + ", '" + name+"' ," + String.valueOf(height) + "," + String.valueOf(population) + ")";
            
            System.out.println(query);

            ps = connection.prepareStatement(query);
            ps.executeUpdate();
          }
                    
        catch (SQLException se) {
//            System.err.println("CCSQL Exception," + "<Message>: " + se.getMessage());
            return false;
        }
        return true;
    }
  
    public int getCountriesNextToOceanCount(int oid) throws SQLException {
        try {
            query = "SELECT count(cid) as num FROM a2.oceanAccess WHERE oid=" + String.valueOf(oid);
            ps = connection.prepareStatement(query);
            rs = ps.executeQuery();
            while (rs.next()) {
                int num = rs.getInt("num");
                return num;
            }
            return -1; // never reached but removal of this line results in compilation error
        }
        catch (SQLException se) {
            return -1;
        }  
    }
   
    public String getOceanInfo(int oid) throws SQLException { 
        try {
            query = "SELECT * FROM a2.ocean WHERE oid="+String.valueOf(oid);
            System.out.println(query);
            ps = connection.prepareStatement(query);
            rs = ps.executeQuery();
            String result = "";
            while (rs.next()) {
                result += String.valueOf(rs.getInt("oid")) + ":" + rs.getString("oname") + ":" + String.valueOf(rs.getInt("depth"));
                System.out.println("here");
                return result;
            }
            return "";
        }
        catch (SQLException se) {
            return "";
        }
    }

    public boolean chgHDI(int cid, int year, float newHDI) throws SQLException{
        try {
            query = "SELECT count(*) as num FROM a2.hdi WHERE cid="+String.valueOf(cid) + " and year=" + String.valueOf(year);
            System.out.println(query); 
            ps = connection.prepareStatement(query);
            rs = ps.executeQuery();
            String result = "";
            while (rs.next()) {
                int num = rs.getInt("num");
                System.out.println(num);
                if(num != 1) {
                    System.out.println("here ");
                    return false;
                }
            }
            query ="UPDATE a2.hdi set hdi_score=" + String.valueOf(newHDI) + " WHERE cid="+String.valueOf(cid) + " and year=" + String.valueOf(year);
            System.out.println(query);
            ps = connection.prepareStatement(query);
            ps.executeUpdate();
       	    if (ps.getUpdateCount() == 1) {
		return true;
	    } else {
		return false;
	    }
        }
        catch (SQLException se) {
//            System.err.println("AASQL Exception," + "<Message>: " + se.getMessage());
            return false;
        }
    }

    public boolean deleteNeighbour(int c1id, int c2id) throws SQLException {
        try {
            query = "DELETE FROM a2.neighbour WHERE country = " + String.valueOf(c1id) + " and neighbor = " + String.valueOf(c2id);
            ps = connection.prepareStatement(query);
            ps.executeUpdate();
	    if (ps.getUpdateCount() != 1) {
		return false;
	    }

            query = "DELETE FROM a2.neighbour WHERE country = " + String.valueOf(c2id) + " and neighbor = " + String.valueOf(c1id);
            ps = connection.prepareStatement(query);
            ps.executeUpdate();
            if (ps.getUpdateCount() == 1) {
		return true;
	    } else {
		return false;
	    }
        }
        catch (SQLException se) {
//            System.err.println("AASQL Exception," + "<Message>: " + se.getMessage());
            return false;
          
        }
    }
  
    public String listCountryLanguages(int cid){
        try {
            query = "SELECT l.lid, l.lname, (c.population*l.lpercentage) as p FROM a2.language l, a2.country c WHERE l.cid=c.cid and l.cid=" + String.valueOf(cid) + "  ORDER BY p";
            System.out.println(query);
            ps = connection.prepareStatement(query);
            rs = ps.executeQuery();
            String result = "";
            while (rs.next()) {
                result += String.valueOf(rs.getInt("lid")) + ":" + rs.getString("lname") + ":" + String.valueOf(rs.getFloat("p")) + "#";
                System.out.println("here"); 
            }
            if (result != "") {
	        return result.substring(0,result.length()-1);
	    } else {
	        return "";
	    }
        }
        catch (SQLException se) {
//            System.err.println("AASQL Exception," + "<Message>: " + se.getMessage());
            return "";
        }
    }
  
    public boolean updateHeight(int cid, int decrH) throws SQLException{
        try {
            int height = -1;
            query = "SELECT height FROM a2.country WHERE cid=" + String.valueOf(cid);
            ps = connection.prepareStatement(query);
            rs = ps.executeQuery();
            while (rs.next()) {
                height = rs.getInt("height");
            }
            if (height == -1) {
                return false;
            } else {
                query = "UPDATE a2.country SET height=" + String.valueOf(height-decrH) + "WHERE cid=" + String.valueOf(cid);
                ps = connection.prepareStatement(query);
                ps.executeUpdate();
                if (ps.getUpdateCount() == 1) {
		    return true;
	        } else {
		    return false;
	    	}
            }
        }
        catch (SQLException se) {
            return false;
        }   
    }
    
    public boolean updateDB() throws SQLException{
        try {
          //@374
          //query = "drop table mostPopulousCountries";
          //ps = connection.prepareStatement(query);
          //ps.executeUpdate();
            query = "CREATE table a2.mostPopulousCountries (cid INTEGER, cname VARCHAR(20))";
            ps = connection.prepareStatement(query);
            ps.executeUpdate();
            query = "INSERT INTO a2.mostpopulousCountries (SELECT cid, cname FROM a2.country WHERE population > 100e6)";
            ps = connection.prepareStatement(query);
            ps.executeUpdate();
            return true;
            }
        catch (SQLException se) {
            return false;    
        }   
    }
 

   /* public static void main(String[] argv) throws ClassNotFoundException, SQLException {
        Assignment2 a = new Assignment2();
        System.out.println("inside");
        a.connectDB("jdbc:postgresql://localhost:5432/csc343h-g3mathol", "g3mathol", "");
        System.out.println("Insert Narnia:" + a.insertCountry(4, "Narnia", 7895, 9000000));
//        a.query = "SELECT * from a2.country";
//        a.ps = a.connection.prepareStatement(a.query);
//        a.rs = a.ps.executeQuery();

//        while (a.rs.next()) {
//            String name = a.rs.getString("cname");
//            System.out.println(name);
//        }

        int res = a.getCountriesNextToOceanCount(11);
        System.out.println("Count countries next to ocean 11:  " + res);
        System.out.println("Ocean info:  " + a.getOceanInfo(11));
        System.out.println("Change hdi:  " + a.chgHDI(1, 1990, 35));
        System.out.println("Delete Neighbour:  " + a.deleteNeighbour(1, 2));
        System.out.println("Languages for country 1:   " + a.listCountryLanguages(1));
	System.out.println("Non-existing country languages  " + a.listCountryLanguages(7));
        System.out.println("Update height:  " + a.updateHeight(4, 500));
        //System.out.println(a.updateDB());
    
    }*/

} 

