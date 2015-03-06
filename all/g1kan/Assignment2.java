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

  // Checks if the Driver is loaded.
  boolean loaded;
  
  //CONSTRUCTOR
  Assignment2(){
	connection = null;
	sql = null;
	ps = null;
	rs = null;
	try{
		Class.forName("org.postgresql.Driver");
		loaded = true;
	}catch(ClassNotFoundException e){
		loaded = false;
	}
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
	if(loaded == false){
		return false;
	}
	try{
		String url = "jdbc:postgresql://" + URL;
		connection = DriverManager.getConnection(url, username, password);
		return true;
	}catch(SQLException e){
		connection = null;
		return false;
	}
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      	if(connection == null){
		return false;
	}
      	try{
		connection.close();
		connection = null;
		return true;
	}catch(SQLException e){
		return false;
	}
		
  }
    
  // Inserts a tuple into the country relation, if the tuple is not already in the relation.
  public boolean insertCountry(int cid, String name, int height, int population) {
	int update = 0;
	try{
		ps = connection.prepareStatement("INSERT INTO a2.country (cid, cname, height, population) VALUES(?, ?, ?, ?)");
		ps.setInt(1, cid);
		ps.setString(2, name);
		ps.setInt(3, height);
		ps.setInt(4, population);
		update = ps.executeUpdate();
	}catch(SQLException e1){				
		update = -1;
	}finally{
		try{
			ps.close();
		}catch(SQLException e2){
			ps = null;
		}
		if(update == 1){
			return true;
		}else{
			return false;
		}
	}
  }
  
  // Returns the number of counties with direct access to ocean oid.
  public int getCountriesNextToOceanCount(int oid) {
	int num = 0;
	try{
		ps = connection.prepareStatement("SELECT count(*) FROM a2.oceanAccess where oid=? GROUP BY oid");
		ps.setInt(1, oid);
		rs = ps.executeQuery();
		if(rs.next()){
			num = rs.getInt(1);
		}
	}catch(SQLException e1){
		num = -1;
	}finally{
		try{
			ps.close();
		}catch(SQLException e2){
			ps = null;
		}
		try{
			rs.close();
		}catch(SQLException e3){
			rs = null;
		}
		return num;
	} 
  }
   
  // Gets info about ocean with given oid and returns tuple as a string.
  public String getOceanInfo(int oid){
	String str = "";
	try{
		ps = connection.prepareStatement("SELECT * FROM a2.ocean where oid=?");
		ps.setInt(1, oid);
		rs = ps.executeQuery();
		if(rs.next()){
			str += rs.getString(1);
			str += ":";
			str += rs.getString(2);
			str += ":";
			str += rs.getString(3);
		}
	}catch(SQLException e1){
		str = "";
	}finally{
		try{
			ps.close();
		}catch(SQLException e2){
			ps = null;
		}
		try{
			rs.close();
		}catch(SQLException e3){
			rs = null;
		}
		return str;
	}   
  }

  // Updates the HDI of a tuple with given cid and year to be newHDI.
  public boolean chgHDI(int cid, int year, float newHDI){
	int update = 0;	
	try{
		ps = connection.prepareStatement("UPDATE a2.hdi set hdi_score=? WHERE cid=? and year=?");
		ps.setFloat(1, newHDI);
		ps.setInt(2, cid);
		ps.setInt(3, year);
		update = ps.executeUpdate();
	}catch(SQLException e1){				
		update = -1;
	}finally{
		try{
			ps.close();
		}catch(SQLException e2){
			ps = null;
		}
		if(update == 1){
			return true;
		}else{
			return false;
		}
	}
  }

// Deletes tuple in neighbour relation where county=c1id and neighbor=c2id.
  public boolean deleteNeighbour(int c1id, int c2id){
	int update = 0;	
	try{
		ps = connection.prepareStatement("DELETE FROM a2.neighbour WHERE country=? and neighbor=?");
		ps.setInt(1, c1id);
		ps.setInt(2, c2id);
		update = ps.executeUpdate();
		ps = connection.prepareStatement("DELETE FROM a2.neighbour WHERE country=? and neighbor=?");
		ps.setInt(1, c2id);
		ps.setInt(2, c1id);
		update = update + ps.executeUpdate();
	}catch(SQLException e1){				
		update = -1;
	}finally{
		try{
			ps.close();
		}catch(SQLException e2){
			ps = null;
		}
		if(update == 2){
			return true;
		}else{
			return false;
		}
	}
  }      
  
  // Returns a list of languages spoken in the country with the given cid.
  public String listCountryLanguages(int cid){
	String str = "";
	try{
		String query = "SELECT lid, lname, (population*lpercentage) as lpopulation FROM a2.country c, a2.language l";
		query = query + " where c.cid=? and c.cid=l.cid order by lpopulation";
		ps = connection.prepareStatement(query);
		ps.setInt(1, cid);
		rs = ps.executeQuery();
		boolean rows = rs.next();
		while(rows){
			str += rs.getString(1) + ":" + rs.getString(2) + ":" + rs.getString(3);
			rows = rs.next();
			if(rows){
				str += "#";
			}
		}
	}catch(SQLException e1){
		str = "";
	}finally{
		try{
			ps.close();
		}catch(SQLException e2){
			ps = null;
		}
		try{
			rs.close();
		}catch(SQLException e3){
			rs = null;
		}
		return str;
	} 
  }
  
  public boolean updateHeight(int cid, int decrH){
	int update = 0;    	
	try{
		int h = 0;
		ps = connection.prepareStatement("SELECT height FROM a2.country where cid=?");
		ps.setInt(1, cid);
		rs = ps.executeQuery();
		if(rs.next()){
			h = rs.getInt(1);
		}
		h = h - decrH;
		ps = connection.prepareStatement("UPDATE a2.country set height=? WHERE cid=?");
		ps.setInt(1, h);
		ps.setInt(2, cid);
		update = ps.executeUpdate();
	}catch(SQLException e1){				
		update = -1;
	}finally{
		try{
			ps.close();
		}catch(SQLException e2){
			ps = null;
		}
		if(update == 1){
			return true;
		}else{
			return false;
		}
	}
  }
    
  // Update Database with the mostPopulousCountries table.
  public boolean updateDB(){
	boolean create = true;
	try{
		sql = connection.createStatement();
		String query = "SELECT cid, cname INTO a2.mostPopulousCountries FROM a2.country WHERE population>100000000;";
		sql.executeUpdate(query);
	}catch(SQLException e1){
		create = false;
	}finally{
		try{
			sql.close();
		}catch(SQLException e2){
			sql = null;
		}
		return create;
	}  
  }
  
}
