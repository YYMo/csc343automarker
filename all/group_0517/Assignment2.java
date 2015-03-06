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
    Assignment2() throws ClassNotFoundException{
	Class.forName("org.postgresql.Driver");
    }
    
    //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
    public boolean connectDB(String URL, String username, String password){
	try{
	    connection = DriverManager.getConnection(URL, username, password);
	}
	catch(SQLException e){
	    return false;
	}
	return true;
    }
    
    //Closes the connection. Returns true if closure was sucessful
    public boolean disconnectDB(){
	try{
	    if (ps != null){
		connection.close();
	    }
	    if (connection != null){
		connection.close();
	    }
	}
	catch(SQLException e){
	    return false;    
	}
	return true;
    }
	
    public boolean insertCountry (int cid, String name, int height, int population) {
	try{
	    // Check if cid already exists in the country table. 
	    String string1 = "SELECT * FROM a2.country WHERE cid = ?";
	    ps = connection.prepareStatement(string1);
	    ps.setInt(1, cid);
	    rs = ps.executeQuery();
	    if (rs.next()){
		return false;
	    }
	    String string2 = "INSERT INTO a2.country VALUES (?, ?, ?, ?)";
	    ps = connection.prepareStatement(string2);
	    ps.setInt(1, cid);
	    ps.setString(2, name);
	    ps.setInt(3, height);
	    ps.setInt(4, population);
	    ps.executeUpdate();
	}
	catch(SQLException e1){
	    return false;
	}
	return true;
    }
    
    public int getCountriesNextToOceanCount(int oid) {
	try{
	    String string = "SELECT COUNT(cid) AS numCountries FROM a2.oceanAccess WHERE oid = ?";
	    ps = connection.prepareStatement(string);
	    ps.setInt(1, oid);
	    rs = ps.executeQuery();
	    if (rs.next()){
		return rs.getInt(1);
	    }
	    return 0;
	}
	catch(SQLException e1){
	    return -1;
	}
    }
    
    public String getOceanInfo(int oid) {
	try{
	    String string = "SELECT oid, oname, depth FROM a2.ocean WHERE oid = ?";
	    ps = connection.prepareStatement(string);
	    ps.setInt(1, oid);
	    rs = ps.executeQuery();
	    if (rs.next()){
		String id = Integer.toString(rs.getInt(1));
		String oname = rs.getString(2);
		String depth = Integer.toString(rs.getInt(3));
		return id+":"+oname+":"+depth;
	    }
	    return "";
	}
	catch(SQLException e){
	    return "";
	}
    }

    public boolean chgHDI(int cid, int year, float newHDI){
	try{
	    // Check if the record of according cid and year exists.
	    String string1 = "SELECT * FROM a2.hdi WHERE cid = ? AND year = ?";
	    ps = connection.prepareStatement(string1);
	    ps.setInt(1, cid);
	    ps.setInt(2, year);
	    rs = ps.executeQuery();
	    if (!rs.next()){
		return false;
	    }
	    String string2 = "UPDATE a2.hdi SET hdi_score = ? WHERE cid= ? AND year= ?";
	    ps = connection.prepareStatement(string2);
	    ps.setFloat(1, newHDI);
	    ps.setInt(2, cid);
	    ps.setInt(3, year);
	    ps.executeUpdate();
	}
	catch(SQLException e1){
	    return false;
	}
	return true;
    }

    public boolean deleteNeighbour(int c1id, int c2id){
	try{
	    // Check if neighbour exists.
	    String string1 = "SELECT * FROM a2.neighbour WHERE country = ? AND neighbor = ?";
	    ps = connection.prepareStatement(string1);
	    ps.setInt(1, c1id);
	    ps.setInt(2, c2id);
	    rs = ps.executeQuery();
	    if (!rs.next()){
		return false;
	    }
	    String string2 = "DELETE FROM a2.neighbour WHERE country = ? AND neighbor = ?";
	    ps = connection.prepareStatement(string2);
	    ps.setInt(1, c1id);
	    ps.setInt(2, c2id);
	    ps.executeUpdate();
	    ps.setInt(1, c2id);
	    ps.setInt(2, c1id);
	    ps.executeUpdate();
	}
	catch(SQLException e1){
	    return false;
	}
	return true;
    }
    
    public String listCountryLanguages(int cid) {
	try{
	    String string = "SELECT lid, lname, (lpercentage * population) AS pop FROM a2.language NATURAL JOIN a2.country WHERE cid = ? ORDER BY pop";
	    ps = connection.prepareStatement(string);
	    ps.setInt(1, cid);
	    rs = ps.executeQuery();
	    String finalString = "";
	    while (rs.next()){
		String lid = Integer.toString(rs.getInt(1));
		String lname = rs.getString(2);
		String population = Integer.toString(rs.getInt(3));
		finalString += lid+":"+lname+":"+population+"#";
	    }
	    if (finalString != "" && finalString.charAt(finalString.length()-1) == '#'){
		finalString = finalString.substring(0, finalString.length()-1);
	    }
	    return finalString;
	}
	catch (SQLException e){
	    return "";
	}
    }
    
    public boolean updateHeight(int cid, int decrH){
	try{
	    // Check if country exists.
	    String string1 = "SELECT * FROM a2.country WHERE cid = ?";
	    ps = connection.prepareStatement(string1);
	    ps.setInt(1, cid);
	    rs = ps.executeQuery();
	    if (!rs.next()){
		return false;
	    }
	    String string2 = "UPDATE a2.country SET height = height-? WHERE cid = ?";
	    ps = connection.prepareStatement(string2);
	    ps.setInt(1, decrH);
	    ps.setInt(2, cid);
	    ps.executeUpdate();
	}
	catch(SQLException e1){
	    return false;
	}
	return true;
    }
	
    public boolean updateDB(){
	try{
	    String string1 = "DROP TABLE IF EXISTS a2.mostPopulousCountries CASCADE";
	    ps = connection.prepareStatement(string1);
	    ps.executeUpdate();
	    String string2 = "CREATE TABLE a2.mostPopulousCountries " +
			    "(cid INTEGER, " +
			    "cname VARCHAR(20))";
	    ps = connection.prepareStatement(string2);
	    ps.executeUpdate();
	    String string3 = "INSERT INTO a2.mostPopulousCountries (cid, cname) SELECT cid, cname FROM a2.country WHERE population>100000000 ORDER BY cid ASC";
	    ps = connection.prepareStatement(string3);
	    ps.executeUpdate();
	}
	catch(SQLException e){
	    return false;
	}
	return true;
    }
}
