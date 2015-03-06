import java.sql.*;
public class Assignment2{

    // A connection to the database  
    Connection connection;

    // Statement to run queries
    Statement sql;

    // Prepared Statement
    PreparedStatement ps;

    // Resultset for the query
    ResultSet rs;

    //CONSTRUCTOR
    Assignment2() {
        try {
            Class.forName("org.postgresql.Driver");
        }
        catch (ClassNotFoundException e) {
        }
    }

    //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
    public boolean connectDB(String URL, String username, String password) {

        try {
            connection = DriverManager.getConnection(URL, username, password);
            return true;
        } catch (SQLException se) {
            return false;
        }
        
    }

    //Closes the connection. Returns true if closure was sucessful
    public boolean disconnectDB() {
        try {
            connection.close();
            return true;
        } catch (SQLException se) {
            return false;
        }
    }

    // does a ; need to be added at the end of the query?

    public boolean insertCountry(int cid, String name, int height, int population) {
        try {

            ps = connection.prepareStatement(
                "INSERT INTO a2.country VALUES (?, ?, ?, ?)");
            ps.setInt(1, cid);
            ps.setString(2, name);
            ps.setInt(3, height);
            ps.setInt(4, population); 
            ps.executeUpdate();
            
            ps.close();
            // do the check here
            return true;
        }
        catch (SQLException se)
        {
            return false;
            
        }
    }

    public int getCountriesNextToOceanCount(int oid) {
        try {
            ps = connection.prepareStatement(
                "SELECT count(*) as amount" +  
                "FROM a2.oceanAccess " + 
                "WHERE oceanAccess.oid = ?");
            ps.setInt(1, oid);
            rs = ps.executeQuery();
            int total = rs.getInt("amount");
            ps.close();
            rs.close();
            return total;

        } 
        catch (SQLException se){
            return -1;
        }
    }

    public String getOceanInfo(int oid) {
        try {
            String result = "";
            ps = connection.prepareStatement(
                "SELECT ocean.oid, ocean.oname, ocean.depth " + 
                 "FROM a2.ocean " + 
                 "WHERE ocean.oid = ?");
            ps.setInt(1, oid);
            rs = ps.executeQuery();
            
            if (rs.next()){
                int id = rs.getInt("oid");
                String name = rs.getString("oname");
	        int depth = rs.getInt("depth");  
                result = String.valueOf(id) + ":" + name + ":" + String.valueOf(depth);
            }
            ps.close();
            rs.close();
            return result;
        } 
        catch (SQLException se){
            return "";
        }
    }

    public boolean chgHDI(int cid, int year, float newHDI) {
        try {
            
            ps = connection.prepareStatement( 
                "UPDATE a2.hdi" +  
                "SET hdi.hdi_score = ?" +  
                "WHERE hdi.cid = ? AND hdi.year = ?");
                       
            ps.setFloat(1, newHDI); 
            ps.setInt(2, cid);
            ps.setInt(3, year);    
            ps.executeUpdate(); 
             
            ps.close();
            return true;
        } 
        catch (SQLException se){
 
            return false;
        } 
    }

    public boolean deleteNeighbour(int c1id, int c2id) {
        try {
            
            ps = connection.prepareStatement( 
                "DELETE FROM a2.neighbour " + 
                 "WHERE ((neighbour.country = ? AND neighbour.c = ?) OR (neighbour.country = ? AND neighbour.neighbor = ?)");
                    
            ps.setInt(1, c1id);
            ps.setInt(2, c2id); 
            ps.setInt(3, c2id);
            ps.setInt(4, c1id);
            ps.executeUpdate();
            ps.close();
            return true;
        } 
        catch (SQLException se){
 
            return false;
        } 
    }

    public String listCountryLanguages(int cid) {
        try {
            String result = "";
            ps = connection.prepareStatement(
                "SELECT language.lid, language.lname, language.lpercentage * " + 
                "(SELECT country.population FROM a2.country WHERE country.cid = ?) as speakers " + 
                "FROM a2.language " +  
                "WHERE language.cid = ? " + 
                "ORDER BY speakers");


            ps.setInt(1, cid);
            ps.setInt(2, cid);
            rs = ps.executeQuery();
           
     
            if (rs.next()){
              
		int id = rs.getInt("lid");
	    String name = rs.getString("lname");
		float numspeakers = rs.getFloat("speakers");
		result = result + String.valueOf(id) + ":" + name + ":" + String.valueOf(numspeakers);
                    
            }
 
            while (rs.next()) { 
                int id = rs.getInt("lid");
                String name = rs.getString("lname");
                float numspeakers = rs.getFloat("speakers");
                result = result + "#" + String.valueOf(id) + ":" + name + ":" + String.valueOf(numspeakers);
            } 

           
            ps.close();
            rs.close();
            return result;
        }
        catch (SQLException se){
            return "";
        }       
    }

    public boolean updateHeight(int cid, int decrH) {
        try {   
            ps = connection.prepareStatement( 
                "UPDATE a2.country " + 
                "SET country.height = ? " + 
                "WHERE country.cid = ?");
                     
            ps.setInt(1, cid);
            ps.setInt(2, decrH);    
            ps.executeQuery();
            ps.close();
            return true;
        } 
        catch (SQLException se){
            return false;
        } 
    }

    public boolean updateDB() { 
        try {
            // does it need a2 at the beginning or not?
            ps = connection.prepareStatement( 
            "CREATE TABLE IF NOT EXISTS mostPopulousCountries( " + 
            " cid INT " + 
            "cname VARCHAR (20)"); 
            ps.executeUpdate();
            ps.close();

            ps = connection.prepareStatement(
            "INSERT INTO a2.mostPopulousCountries ( " +   
            "SELECT country.cid, country.cname  " + 
            "FROM a2.country " + 
            "WHERE population > 100000000 " + 
            "ORDER BY country.cid ASC)");
            ps.executeUpdate();
            ps.close();
            // update might not work because a query contained within
            return true;
        }
        catch (SQLException se){ 
            return false;
        }
    }

}