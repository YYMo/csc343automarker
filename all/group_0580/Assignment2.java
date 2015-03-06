import java.sql.*;

public class Assignment2 {
    // A connection to the database  
    Connection connection;
  
    // Statement to run queries
    Statement sql;
    Statement sql1;
  
    // Prepared Statement
    PreparedStatement ps;
  
    // Resultset for the query
    ResultSet rs;
  
    //CONSTRUCTOR
    Assignment2(){
		try{
			//Load JDBC Driver
			Class.forName("org.postgresql.Driver");
			connection = null;
			sql = null;
			sql1 = null;
			ps = null;
			rs = null;
		}catch (Exception e){

		}
    }

    public String ocean_rs_to_string(ResultSet rs){
		String retStr = "";
		try{
			if (rs != null){
				while (rs.next()){
					retStr += rs.getInt("oid") + ":" + rs.getString("oname") + ":" + rs.getInt("depth") + "#";
				}
				if (retStr.length() > 0) retStr = retStr.substring(0, retStr.length() - 1);
			}
			return retStr;
		}catch(Exception e){
			return "";
		}
    }
	
	public String CLang_rs_to_string(ResultSet rs){
		String retStr = "";
		try{
			if (rs != null){
				while (rs.next()){
					retStr += rs.getInt("lid") + ":" + rs.getString("lname") + ":" + rs.getInt("lpopulation") + "#";
				}
				if (retStr.length() > 0) retStr = retStr.substring(0, retStr.length() - 1);
			}
			return retStr;
		}catch (Exception e){
			return "";
		}
	}
  
    //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
    public boolean connectDB(String URL, String username, String password){
		try{
			connection = DriverManager.getConnection("jdbc:postgresql://" + URL, username, password);
			if (connection != null){
				return true;
			}else{
				return false;
			}
		}catch (Exception e){
			return false;
		}
    }
  
    //Closes the connection. Returns true if closure was sucessful
    public boolean disconnectDB(){
		try{
			/**************************ADD MORE CLOSING before submission ***************************/
			if (rs != null) rs.close();
			if (ps != null) ps.close();
			if (sql != null) sql.close();
			if (sql1 != null) sql1.close();
			if (connection != null) connection.close();
			return true;
		}catch (Exception e){
			return false;
		}
    }
    
    public boolean insertCountry (int cid, String name, int height, int population) {
		try{
			// if connection is null, there is no connection to the db so immediately return false
			if (connection == null) return false;
		
			String sqlText;

			sqlText = "INSERT INTO a2.country VALUES(?,?,?,?)";
			
			ps = connection.prepareStatement(sqlText);
			
			//Column 1 is cid
			ps.setInt(1, cid);
			
			//Column 2 is name
			ps.setString(2, name);
			
			//Column 3 is height
			ps.setInt(3, height);
			
			//Column 4 is population
			ps.setInt(4, population);

			ps.executeUpdate();
			
			return true;
		}catch (Exception e){
			return false;
		}
    }
  
    public int getCountriesNextToOceanCount(int oid) {
		try{
			if (connection == null) return -1;
			
			int numOfCountries = -1;
			
			sql = connection.createStatement();
			
			String sqlText = "SELECT count(A.cid) AS numCountries FROM a2.oceanAccess AS A where A.oid = " + oid;
			
			rs = sql.executeQuery(sqlText);
			
			while(rs.next()){
				numOfCountries = rs.getInt("numCountries");
			}
			
			if (numOfCountries >= 0){
				return numOfCountries;
			}
			else{
				return -1;
			}	
		}catch (Exception e){
			return -1;
		}
    }
   
    public String getOceanInfo(int oid){
		try{
			if (connection == null) return "";

			sql = connection.createStatement();

			String sqlText = "SELECT * FROM a2.ocean AS A WHERE A.oid = " + oid;	   

			rs = sql.executeQuery(sqlText);

			return ocean_rs_to_string(rs);
		}catch (Exception e){
			return "";
		}
    }

    public boolean chgHDI(int cid, int year, float newHDI){
		try{
			if (connection == null) return false;
			
			sql = connection.createStatement();
			
			String sqlText;
			
			sqlText = "UPDATE a2.hdi AS A" +
					" SET hdi_score = " + newHDI +
					" WHERE A.cid = " + cid + " AND A.year = " + year;
			
			sql.executeUpdate(sqlText);
			
			return true;
		}catch (Exception e){
			return false;
		}
    }

    // Deletes neighbouring relation in neighbour assuming c1id, c2id already exist in neighbour
    public boolean deleteNeighbour(int c1id, int c2id){
		try{
			// if connection is null, there is no connection to the db so immediately return false
			if (connection == null) return false;
			
			sql = connection.createStatement();
		
			String sqlText;

			sqlText = "DELETE FROM  a2.neighbour AS A " + 
					"WHERE (A.country = " + c1id + " AND A.neighbor = " + c2id + ") OR" +
					" (A.country = " + c2id + " AND A.neighbor = " + c1id + ")";

			sql.executeUpdate(sqlText);
			return true;
		}catch (Exception e){
			return false;
		}
    }
  
    public String listCountryLanguages(int cid){
		try{
			if (connection == null) return "";
			
			sql = connection.createStatement();
			
			String sqlText;
			
			sqlText = "SELECT L.lid, L.lname, L.lpercentage*C.population AS lpopulation FROM a2.language AS L, a2.country AS C " +
					"WHERE L.cid = C.cid AND L.cid = " + cid;
			
			rs = sql.executeQuery(sqlText);
			
			return CLang_rs_to_string(rs);
		
		}catch (Exception e){
			return "";
		}
    }
  
    public boolean updateHeight(int cid, int decrH){
		try {
			if (connection == null) return false;

			// decrH has to be positive
			if (decrH < 0) return false;
			
			sql = connection.createStatement();
			
			int height = 0;
			String sqlText;

			sqlText = "SELECT height FROM a2.country AS A " + 
					"WHERE A.cid = " + cid;
			
			rs = sql.executeQuery(sqlText);

			// no result = not successful
			if (rs == null) return false;
			while(rs.next())
				height = rs.getInt("height");
			
			// decrement height
			height -= decrH;
			
			sqlText = "UPDATE a2.country AS A" +
					" SET height = " + height +
					" WHERE A.cid = " + cid;
			
			sql.executeUpdate(sqlText);

			return true;
		} catch (Exception e){
			return false;
		}
    }
    
    public boolean updateDB(){
		try {
			if (connection == null) return false;

			sql = connection.createStatement();
			
			String sqlText;
			
			sqlText = "CREATE TABLE IF NOT EXISTS a2.mostPopulousCountries(" +
					"cid INTEGER REFERENCES a2.country(cid) ON DELETE RESTRICT," + 
					"cname VARCHAR(20) NOT NULL," + "PRIMARY KEY (cid))";
			
			sql.executeUpdate(sqlText);
	 
			sqlText = "SELECT cid, cname FROM a2.country WHERE population > 100000000 ORDER BY cid ASC";
			rs = sql.executeQuery(sqlText);

			sql1 = connection.createStatement();
			if (rs!=null){
				while (rs.next()){
					sqlText = "INSERT INTO a2.mostPopulousCountries VALUES(" +
							rs.getInt("cid") + ", '" +
							rs.getString("cname") + "')";
					sql1.executeUpdate(sqlText);
				}
			}

			return true;
		}catch (Exception e){
			return false;
		}    
    }

    /*****************************************************COMMENT THIS OUT BEFORE SUBMISSION ********************************************/
/*    
	public static void main(String args[]){
		try{
			Assignment2 assn2 = new Assignment2();
			System.out.println("Insert Country: \t" + assn2.insertCountry(101, "Country101", 1035, 1000333));
			System.out.println("Get Count of Countries next to Ocean: \t" + assn2.getCountriesNextToOceanCount(4));
			System.out.println("Get Count of Countries next to Ocean: \t" + assn2.getCountriesNextToOceanCount(6));
			System.out.println("Get Ocean Info: \t" + assn2.getOceanInfo(5));
			System.out.println("Get Ocean Info: \t" + assn2.getOceanInfo(6));
			System.out.println("Change HDR Value for Country: \t" + assn2.chgHDI(6, 2013, 1));
			System.out.println("Delete Neighbour: \t" + assn2.deleteNeighbour(3, 38));
			System.out.println("List Country's Languages: \t" + assn2.listCountryLanguages(6));
			System.out.println("Update Height: \t" + assn2.updateHeight(1, 10));
			System.out.println("Update DB: \t" + assn2.updateDB());
			System.out.println("Disconnect: \t" + assn2.disconnectDB());
		}catch(Exception e){
			System.out.println("Error in main()");
		}
    }*/
}
