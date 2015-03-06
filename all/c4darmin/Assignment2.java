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
 			// Load JDBC driver
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return;
		}
		
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
      try {
 		//Make the connection to the database
		connection = DriverManager.getConnection(URL,username, password);
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		if (connection != null) {

				//Create a Statement for executing SQL queries
				try {
					sql = connection.createStatement(); 
					sql.executeUpdate("SET search_path to A2");
					return true;
				
				} catch (SQLException e) {
					return false;
				}
		} else {
			//System.out.println("Failed to make connection!");
			return false;
		}
  }
  
  //Closes the connection. Returns true if closure was successful
  public boolean disconnectDB(){
      try {
		  connection.close();
		  return true;
		} catch (Exception e){
			return false;
		}
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
   
	try {
		sql = connection.createStatement(); 
		String sqlText;
		sqlText = "INSERT INTO country (cid, cname, height, population) " +
			  "VALUES ("+ cid +" , " + "'" + name + "'"+" , "+ height +" , "+ population +")";
		sql.executeUpdate(sqlText);
		return true;
	} catch (Exception e) {
		e.printStackTrace();
		return false;
	}
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	try {
		String sqlText;
		sqlText = "select count(cid) from oceanaccess group by oid having oid="+ oid;
		//sqlText = " SELECT * FROM oceanaccess ";
		rs = sql.executeQuery(sqlText);
		if (rs != null){
			while (rs.next()){
				return rs.getInt(1);
		}} return -1;
	} catch (Exception e) {
		return -1;  
	}


	/*
	select count(cid) from oceanaccess
	group by oid
	having oid=2;
	*/
  }
   
  public String getOceanInfo(int oid){
   	try {
		String sqlText;
		sqlText = "select * from ocean where oid="+oid;
		rs = sql.executeQuery(sqlText);
		if (rs != null){
			while (rs.next()){
				return rs.getString(1) + ":" + rs.getString(2) + ":" + rs.getString(3);
		}} return "";
	} catch (Exception e) {
		return "";  
	}
  }

  public boolean chgHDI(int cid, int year, float newHDI){
	try {
		String sqlText;
		sqlText = "UPDATE hdi SET hdi_score=" + newHDI + " WHERE (cid=" + cid + " and year=" + year + ")";
		sql.executeUpdate(sqlText);
 		return true;
	} catch (Exception e) {
		return false;  
	}

  }

  public boolean deleteNeighbour(int c1id, int c2id){
   try {
	String sqlText;
		sqlText = "DELETE FROM neighbour WHERE country=" + c1id + "and neighbor=" + c2id;
		sql.executeUpdate(sqlText);
		sqlText = "";
		sqlText = "DELETE FROM neighbour WHERE country=" + c2id + "and neighbor=" + c1id;
		sql.executeUpdate(sqlText);
 		return true;
	} catch (Exception e) {
		return false;  
	}      
  }
  
  public String listCountryLanguages(int cid){
	try {
		String sqlText;
		String output = "";

		sqlText = "Select lid, lname, (population * lpercentage) as population FROM (select * from country join language on country.cid=language.cid where country.cid=" + cid + ") as inter ORDER BY population DESC";
		rs = sql.executeQuery(sqlText);
		if (rs != null){
			while (rs.next()){
				output += "#" + rs.getString(1) + ":" + rs.getString(2) + ":" + rs.getString(3) + ":";
		}} return output;
	} catch (Exception e) {
		return "";  
	}

	/*
Select lid, lname, (population * lpercentage) as population FROM
(select * from country join language on country.cid=language.cid where country.cid=1) as inter
ORDER BY population DESC;



*/

  }


  public boolean updateHeight(int cid, int decrH){
    	//Upate height of a single row in table country
	
	int height = getHeight(cid);
	try {
	String sqlText;
	sqlText = "UPDATE country      " +
                  "   SET height =" + (height-decrH) + " " + 
                  " WHERE  cid =" + cid; 

	sql.executeUpdate(sqlText);
	return true;
	}
	catch (Exception e) {
		return false;
	}
  }

  private int getHeight(int cid){
	try {
		String sqlText;
		sqlText = "select height from country where cid="+cid;
		rs = sql.executeQuery(sqlText);
		if (rs != null){
			while (rs.next()){
				return rs.getInt(1);
		}} return -1;
	} catch (Exception e) {
		return -1;  
	}
  }
    
  public boolean updateDB(){
	try {
		//Create jdbc_demo table
		String sqlText;
		sqlText = "CREATE TABLE mostPopulousCountries(                  " +
			  "                       cid INTEGER,        " +
			  "                       cname VARCHAR(20) " +
			  "                      ) ";
		sql.executeUpdate(sqlText);
		System.out.println("TEST ACHIEVED");


		sqlText = "select cid, cname from country where population > 100000000 ORDER BY population ASC";
		rs = sql.executeQuery(sqlText);
		sqlText = "INSERT INTO mostPopulousCountries " + 
			  "VALUES (?,?)  ";
		ps = connection.prepareStatement(sqlText);
		int count = 0;
		if (rs != null){
			while (rs.next()){
				ps.setInt(1, rs.getInt(1));
				ps.setString(2, rs.getString(2));
				ps.executeUpdate();
		}}


	} catch (Exception e) {
		e.printStackTrace();
		System.out.println("TEST FAIL");
		return false;  
	} return false;
  } 
  
}
