import java.sql.*;
import java.io.*;

public class Assignment2 {
    
    // A connection to the database  
    Connection connection;
    
    // Statement to run queries
    Statement sql;
    
    // Prepared Statement
    PreparedStatement ps;
    
    // Resultset for the query
    ResultSet rs;

    // String for query
    String qs;
    
    //CONSTRUCTOR
    //Identifies the postgreSQL driver
    Assignment2(){
        try {
          Class.forName("org.postgresql.Driver");
        }          
        catch (ClassNotFoundException e) {
          System.out.println("Failed to find the JDBC driver");
        }
    }

    //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
    public boolean connectDB(String URL, String username, String password){
        try {
            connection = DriverManager.getConnection(URL, username, password);
	    qs = "set search_path to a2";
	    ps = connection.prepareStatement(qs);
	    ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("SQL Exception." +
                        "<Message>: " + e.getMessage());
            return false;
        }
        return true;
        
    }
    
    //Closes the connection. Returns true if closure was sucessful
    public boolean disconnectDB(){
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                System.err.println("SQL Exception." +
                        "<Message>: " + e.getMessage());
                return false;
            }
            return true;
        } else {
        return false;
        }
    }
      
    public boolean insertCountry (int cid, String name, int height, int population) {
        // checks whether this cid is already in the table
        try {
            qs = "select c1.cid from country c1";
            ps = connection.prepareStatement(qs);
            rs = ps.executeQuery();
            while (rs.next()) {
                if (cid == rs.getInt(1)) {
                    return false;
                }
            }
            qs = "insert into country values (?,?,?,?)";
            ps = connection.prepareStatement(qs);
	    ps.setInt(1, cid);
	    ps.setString(2, name);
	    ps.setInt(3, height);
	    ps.setInt(4, population);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("SQL Exception." +
                        "<Message>: " + e.getMessage());
            return false;
        }
        return true;
    }
    
    public int getCountriesNextToOceanCount(int oid) {
        //int counter;
        try {
            qs = "select count(*) from oceanAccess "
		+ "group by oid "
		+ "having oid =" + oid;
            ps = connection.prepareStatement(qs);
            rs = ps.executeQuery();
	    if (rs.next()) {
	        return rs.getInt(1);
	    } else {
	    	return -1;
            }
        } catch (SQLException e) {
            System.err.println("SQL Exception." +
                        "<Message>: " + e.getMessage());
            return -1;
        }
    }
     
    public String getOceanInfo(int oid){
        String info;
        int oceanid;
        String oceanName;
        int oceandepth;
        try {
            qs = "select * from ocean where oid = " + oid;
            ps = connection.prepareStatement(qs);
            rs = ps.executeQuery();
	    if (rs.next()) {
            	oceanid = rs.getInt(1);
            	oceanName = rs.getString(2);
            	oceandepth = rs.getInt(3);
	    } else {
	    	return "";
	    }
            if (oceanid == 0 && oceanName == null && oceandepth == 0) {
                return "";
            } else {
                info = oceanid + ":" + oceanName + ":" + oceandepth;
                return info;
            }
        } catch (SQLException e) {
            System.err.println("SQL Exception." +
                        "<Message>: " + e.getMessage());
            return "";
        }
    }

    public boolean chgHDI(int cid, int year, float newHDI){
        try {
            qs = "update hdi set hdi_score = " + newHDI + 
	    "where cid = " + cid + "and year = "  + year;
            ps = connection.prepareStatement(qs);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("SQL Exception." +
                        "<Message>: " + e.getMessage());
            return false;
        }
        return true;
    }

    public boolean deleteNeighbour(int c1id, int c2id){
        try {
            qs = "delete from neighbour where country = " + c1id 
	         + "and neighbor ="  + c2id;
            ps = connection.prepareStatement(qs);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("SQL Exception." +
                        "<Message>: " + e.getMessage());
            return false;
        }
        try {
            qs = "delete from neighbour where country =" + c2id 
	        + "and neighbor ="  + c1id;
            ps = connection.prepareStatement(qs);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("SQL Exception." +
                        "<Message>: " + e.getMessage());
            return false;
        }
        return true;     
    }
    
    public String listCountryLanguages(int cid){
        int langId;
        String langName;
        int cPop;
        int langPop;
        double lPct;
        String result = "";
        try {
            qs = "select lid, lname, (lpercentage * population) as lpop " + 
		"from language natural join country where cid = " + cid 
		+ "order by lpop";
            ps = connection.prepareStatement(qs);
            rs = ps.executeQuery();
            if (rs == null) {
                return "";
            } else {
                while (rs.next()) {
                    langId = rs.getInt(1);
                    langName = rs.getString(2);
                    //cPop = rs.getInt("c1.population");
                    //lPct = rs.getDouble("l1.lpercentage");
                    langPop = rs.getInt(3);
                    result = result + langId + ":" + langName + ":" + langPop + "#";
                }
                return (result.substring(0, result.length() - 1));
            }
        } catch (SQLException e) {
            System.err.println("SQL Exception." +
                        "<Message>: " + e.getMessage());
            return "";
        }

    }
    
    public boolean updateHeight(int cid, int decrH){
        try {
            qs = "update country set height = height - " + decrH 
		  + "where cid = " + cid ;
            ps = connection.prepareStatement(qs);
            ps.executeUpdate();
        } catch (SQLException e) {
            return false;
        } 
        return true;
    }
      
    public boolean updateDB(){
        try {
            qs = "create table if not exists mostPopularCountries " 
		 + "(cid integer, cname varchar(20))" ;
            ps = connection.prepareStatement(qs);
            ps.executeUpdate();
            String qstring = "insert into mostPopularCountries (cid, cname) " 
			     + "(select cid, cname from country " 
			     + "where population > 10e8 order by cid)";
            PreparedStatement pstatement = connection.prepareStatement(qstring);
            pstatement.executeUpdate();
        } catch (SQLException e) {
            System.err.println("SQL Exception." +
                        "<Message>: " + e.getMessage());
            return false;
        } 
        return true;    
    }

    //public static void main(String[] args) throws IOException{
        //Assignment2 query = new Assignment2();
        //System.out.println(query.connectDB("jdbc:postgresql://localhost:5432/csc343h-g3brentl", "g3brentl", ""));
        //System.out.println(query.insertCountry(13, "c2", 25, 30));
	//System.out.println(query.getCountriesNextToOceanCount(10));
	//System.out.println(query.getOceanInfo(2));
	//System.out.println(query.chgHDI(3, 2008, (float)0.1));
	//System.out.println(query.deleteNeighbour(1, 10));
	//System.out.println(query.listCountryLanguages(10001));
	//System.out.println(query.updateHeight(1, 3));
	//System.out.println(query.updateDB());
    //}
  
}

