import java.sql.*;
import java.util.*;

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
                	e.printStackTrace();
                 	return;
       		}
 	 }
	/*
  	public static void main(String[] argv){
		String URL = "jdbc:postgresql://localhost:5432/csc343h-c4kimkyv";
		String username = "c4kimkyv";
		String password = "";
		String str;
		Assignment2 test = new Assignment2();
		
		Boolean check;
		check = test.connectDB(URL, username, password);
		System.out.println(check==true);
		
		
		check = test.insertCountry(152, "124a",124,124124);
		System.out.println(check==false); 


		int ocean = test.getCountriesNextToOceanCount(12);
		System.out.println(ocean==-1);


		str = test.getOceanInfo(35);
		System.out.println(str=="");


		check = test.chgHDI(2412, 2012, 0.52f);
		System.out.println(check==false);


		check = test.deleteNeighbour(4, 82);
		System.out.println(check==false);


		str = test.listCountryLanguages(99);
		System.out.println(str);


		check = test.updateHeight(929, 3);
		System.out.println(check==false);

		
		check = test.updateDB();
		System.out.println(check);
		test.disconnectDB();
	}
	*/
	private void close(PreparedStatement r) throws SQLException{
		if (r != null){
			r.close();
		}
	}

	private void close(ResultSet r) throws SQLException{
		if (r != null){
			r.close();
		}
	}

        private void close(Statement r) throws SQLException{
        	if (r != null){
               		r.close();
             	}
        }

        private void close(Connection r) throws SQLException{
        	if (r != null){
              		r.close();
               	}
        }

	/* URL + username or just URL?*/
	public boolean connectDB(String URL, String username, String password){
		try {
			connection = DriverManager.getConnection(URL, username, password);
		} 
		catch (SQLException e) {
			return false;
 		}
      		return true;
  	}
  
  	//Closes the connection. Returns true if closure was sucessful
  	public boolean disconnectDB(){
		try{
			close(connection);
			close(rs);
			close(sql);
			close(ps);
			
		}
		catch (SQLException e){
			return false;
		}
      		return true;    
  	}
    

	/*Testing done*/
	public boolean insertCountry (int cid, String name, int height, int population) {
		try{	
			sql = connection.createStatement();	
			try{
			/*Check if given cid already exists in our database a2.country*/
				String check = "SELECT count(*) FROM a2.country WHERE a2.country.cid="+String.valueOf(cid);
				rs = sql.executeQuery(check);
				if (rs.next()){
					long checker = (long) rs.getObject("count");
					if (checker != 0){
						/*Given cid already exists return false*/
						return false;
					}
				}
				/*Begin Inserting to database a2.country*/
				String sqlText = "INSERT INTO a2.country " + "VALUES" + 
					"(" + String.valueOf(cid) + "," + "'" + name + "'" + "," + 
					String.valueOf(height) + ","  + String.valueOf(population) +  ")";
				if (sql.executeUpdate(sqlText) == 0){
					return false;
				}
			}
			finally{
				close(sql);
				close(rs);
				close(ps);
			}
		}
		catch (SQLException e){
			return false;
		}
 	  	return true;
	}
  	
  	/*Testing done*/
	public int getCountriesNextToOceanCount(int oid) {
		try{
        		sql = connection.createStatement();
			try{
        			String check = "SELECT count(*) FROM a2.oceanAccess WHERE a2.oceanAccess.oid="+String.valueOf(oid);
        			rs = sql.executeQuery(check);
				int count = 0;
        			if (rs != null){
					while (rs.next()){
						count = rs.getInt("count");
					}
				}
				if (count == 0){
					return -1;
				}
				return count;
			}
			finally{
				close(sql);
				close(rs);
				close(ps);
			}			
		}
        	catch (Exception e){    
			return -1;
        	}
	}
   
	/*Testing done*/
	public String getOceanInfo(int oid){
   		try{
                	sql = connection.createStatement();
			try{
                 	       String check = "SELECT * FROM a2.ocean WHERE a2.ocean.oid="+String.valueOf(oid);
                	        rs = sql.executeQuery(check);
				String answer = "";
                      	  	if (rs != null){
                                	while (rs.next()){
						answer = String.valueOf(rs.getInt("oid"))+":"+
						rs.getString("oname")+":"+String.valueOf(rs.getInt("depth"));
                                	}
                        	}
				return answer;
			}
			finally{
				close(sql);
				close(rs);
				close(ps);
			}
                }
                catch (SQLException e){
                        return "";
                }
 	 }

	/*Testing done*/
	public boolean chgHDI(int cid, int year, float newHDI){
		try{
			sql = connection.createStatement();
			try{
				String sqlText = "UPDATE a2.hdi " + "SET hdi_score=" + String.valueOf(newHDI) + 
					" WHERE a2.hdi.cid = " 
					+ String.valueOf(cid) + " AND a2.hdi.year = " + String.valueOf(year);
				if (sql.executeUpdate(sqlText) == 0){
					return false;
				}
				return true;
			}
			finally{
				close(rs);
				close(sql);
				close(ps);
			}
		}
		catch (SQLException e){
			return false;
		}
	}

	/*Testing done*/
	public boolean deleteNeighbour(int c1id, int c2id){
		try{
			sql = connection.createStatement();
			try{
				String sqlText = "DELETE FROM a2.neighbour WHERE (country = " + String.valueOf(c1id) + 
					" AND neighbor = " + String.valueOf(c2id) + ")" + 
				" OR (country = " + String.valueOf(c2id) + " AND neighbor = " + String.valueOf(c1id) + ")";
				if (sql.executeUpdate(sqlText) == 0){
					return false;
				}
				return true;
			}
			finally{
				close(sql);
				close(rs);
				close(ps);
			}
		}
		catch (SQLException e){
			return false;
		}
  	}
  
	/*Testing done*/
	public String listCountryLanguages(int cid){
		try{
			sql = connection.createStatement();
			try{
				String get_pop = "SELECT * FROM a2.country WHERE a2.country.cid = " + String.valueOf(cid);
				rs = sql.executeQuery(get_pop);
				int population = 0;
				if (rs != null){
					while (rs.next()){
						population = rs.getInt("population");
					}
				}
				String sqlText = "SELECT * FROM a2.language WHERE a2.language.cid = " + String.valueOf(cid) + 
						"ORDER BY a2.language.lpercentage";
				rs = sql.executeQuery(sqlText);
				String answer = "";
				String temp;
				String lname;
				if (rs != null){
					while (rs.next()){
						lname = rs.getString("lname");
						lname = lname.trim();
						temp = String.valueOf(rs.getInt("lid")) + ":" +
						lname + ":" + String.valueOf(rs.getFloat("lpercentage") * population) + "#";
						answer = answer + temp;	
					}
				}
				if (answer.length() > 0){
					answer = answer.substring(0, answer.length()-1);
					return answer;
				}
			}
			finally{
				close(sql);
				close(rs);
				close(ps);
			}
		}
		catch (SQLException e){
			return "";
		}
		return "";
  	}
  
	/* Testing done */
	public boolean updateHeight(int cid, int decrH){
                try{
			sql = connection.createStatement();
			try{
				String get_height = "SELECT * FROM a2.country WHERE a2.country.cid = " + String.valueOf(cid);
                  	     	rs = sql.executeQuery(get_height);
                   		int height = 0;
                       		if (rs != null){
                               		while (rs.next()){
                                       		height = rs.getInt("height");
                               		}
                      		}
				int new_height = height - decrH;
                       	 	String sqlText = "UPDATE a2.country " + "SET height=" + new_height +
                               		 " WHERE a2.country.cid = " + String.valueOf(cid);
                     
                        	if (sql.executeUpdate(sqlText) == 0){
					return false;
				}
			}
			finally{
				close(rs);
				close(sql);
				close(ps);
			}
                }
                catch (SQLException e){
                        return false;
                }
                return true;
	}
    

	/*Testing done*/
	public boolean updateDB(){
		try{
			sql = connection.createStatement();
			Statement sql2;
			sql2 = connection.createStatement();
			try{
				String sqlText;
				sqlText = "CREATE TABLE IF NOT EXISTS a2.temp(cid INTEGER, cname VARCHAR(20))";	
				sql2.executeUpdate(sqlText);
                        	String filter = "SELECT * FROM a2.country WHERE a2.country.population > 100000000";
                        	rs = sql.executeQuery(filter);
				int cid;
				String cname;
				String update;
                        	if (rs != null){
                                	while (rs.next()){
                               			cid = rs.getInt("cid");
						cname = rs.getString("cname");
                        			update = "INSERT INTO a2.temp " + "VALUES" +
                                	"(" + String.valueOf(cid) + "," + "'"+cname+"'" +  ")";
						sql2.executeUpdate(update);
					}
                        	}
				sqlText = "CREATE TABLE IF NOT EXISTS a2.mostPopulousCountries(cid INTEGER, cname VARCHAR(20))";
				sql.executeUpdate(sqlText);
				sqlText = "SELECT * FROM a2.temp ORDER BY a2.temp.cid";
				rs = sql.executeQuery(sqlText);
				String insert;
				if (rs != null){
					while (rs.next()){
						cid = rs.getInt("cid");
						cname = rs.getString("cname");
						insert = "INSERT INTO a2.mostPopulousCountries " + 
						"VALUES("+String.valueOf(cid) + "," + "'" + cname + "'" + ")";
						sql2.executeUpdate(insert);
					}
				}
			}
			finally{
				String sqlText = "DROP TABLE a2.temp";
				sql.executeUpdate(sqlText);
				close(rs);
				close(sql);
				close(sql2);
				close(ps);
			}
		}
		catch (SQLException e){
			return false;
		}
		return true;    
	}
  
}
