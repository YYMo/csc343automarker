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
            System.out.println("Load Failed");
            e.printStackTrace();
        }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
        /*
        try {
            // Load JDBC driver
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("Load Failed");
            return false;
        }
        */
        
        try {
            connection = DriverManager.getConnection(URL, username, password);

        } catch (SQLException e) {
            System.out.println("Connection Failed!");
            e.printStackTrace();
            return false;
        }
        
        if (connection != null) {
            return true;
        } else {
            System.out.println("Do not connected");
            return false;
        }

    }

  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
    try {
        this.connection.close();
    } catch (SQLException e) {
        e.printStackTrace();
        System.out.println("Disconnected error");
        return false;
    }
    return true;
    }
    
  public boolean insertCountry (int cid, String name, int height, int population){
	
	try{
	this.sql = connection.createStatement(); 
	} catch (SQLException e) {
		System.out.println("create error");
        return false; 
    }
    
    try{
        String sqlText = "INSERT INTO a2.country(cid, cname, height, population) " + "VALUES(" + cid + ", '" + name + "', " + height + " ," + population + ")" ;
        sql.executeUpdate(sqlText);
    
    } catch (SQLException e) {
        System.out.println("execute insert error");
        return false; 
    }
    return true;
  }



  public int getCountriesNextToOceanCount(int oid){

    int result = 0;
    try{
	this.sql = connection.createStatement(); 
	} catch (SQLException e) {
		System.out.println("create error");
        return -1; 
    }
    
    String sqlText = String.format("SELECT count(*) AS count " + 
                "FROM a2.oceanAccess " +
                "WHERE oid = %d", oid);
    
    try{
        rs = sql.executeQuery(sqlText);
        if (rs != null && rs.next()) {
            return rs.getInt("count");
            } else {
            return 0;   
        }
    } catch (SQLException e){
        e.printStackTrace();
        System.out.println("execute get count error");
        return -1;
    }
  }
   
 
  public String getOceanInfo(int oid){
    String result = "";
    try{
	this.sql = connection.createStatement(); 
	} catch (SQLException e) {
		System.out.println("create error"); 
    }
    
    String sqlText = "SELECT oid, oname, depth " + 
            "FROM a2.ocean " + 
            "WHERE oid =" + oid + ";";
        
        try {
            rs = sql.executeQuery(sqlText);
        } catch (SQLException e) {
		  System.out.println("excute error");
        };
        
        try{
        while (rs.next()) {
            result = (rs.getInt("oid") + ":" + rs.getString("oname") + ":" + rs.getInt("depth"));
			}
		} catch (SQLException e) {
		  System.out.println("get error");
        };
		
        return result;    
  }
  

  public boolean chgHDI(int cid, int year, float newHDI){
        
        try{
		this.sql = connection.createStatement(); 
		} catch (SQLException e) {
		System.out.println("create error"); 
		return false;
		}
               
        String sqlText = String.format("UPDATE a2.hdi "
                + "SET hdi_score=%f "
                + "WHERE cid=%d And year=%d", newHDI, cid, year);
        try {
            sql.executeUpdate(sqlText);
            if (sql.getUpdateCount() < 0) {
                return false;
            }
            return true;
        } catch (SQLException e) {
            System.out.println("execute change error");
            return false;
        }
  }

 
  public boolean deleteNeighbour(int c1id, int c2id){
   
    try{
		this.sql = connection.createStatement(); 
		} catch (SQLException e) {
		System.out.println("create error"); 
		return false;
	}
   
    String sqlText = "DELETE FROM a2.neighbour "
                + "WHERE country=" + c1id + " and neighbor=" + c2id + " or country=" + c2id + " and neighbor=" + c1id;
    
    try {
        sql.executeUpdate(sqlText);
        if (sql.getUpdateCount() < 0) {
            System.out.println("getUpdate deleteNeighbor error");
            return false;
        }
        
        else if (sql.getUpdateCount() == 0) {
			System.out.println("no certain neighbour exist");
		}
        
    } catch (SQLException e) {
            System.out.println("execute deleteNeighbor first");
            return false;
    }

    return true;
     
  }
  

  public String listCountryLanguages(int cid){
	String result="";
    
    try{
		this.sql = connection.createStatement(); 
		} catch (SQLException e) {
		System.out.println("create error"); 
		return result;
	}
    
    String sqlText = "SELECT lid, lname, (population*lpercentage) as pop " +
        "FROM a2.country natural join a2.language " +
        "WHERE cid = " + cid + " ORDER BY pop";

    try {
            rs = sql.executeQuery(sqlText);
            if (rs != null) {
                while (rs.next()) {
                    result += rs.getInt("lid") + ":" + rs.getString("lname") + ":" +rs.getInt("pop") + "#";
                }
            
			}
        } catch (SQLException e) {
			System.out.println("execute list  error");
        };

    return result;
  }
  
  
  public boolean updateHeight(int cid, int decrH){
    
   try{
		this.sql = connection.createStatement(); 
		} catch (SQLException e) {
		System.out.println("create error"); 
		return false;
	}
    
    
    //update country set height = height - derH where cid=cid
    String sqlText = "UPDATE a2.country " + "SET height = height - " + decrH + 
        "WHERE cid= " + cid;

    try {
        sql.executeUpdate(sqlText);
        if (sql.getUpdateCount() < 0) {
            return false;
            }
        else if(sql.getUpdateCount() == 0){
			System.out.println("no such country");
		}
        return true;
        } catch (SQLException e) {
		System.out.println("update height error");
        return false;
    }   
  }
    
 
  public boolean updateDB(){
    try{
		this.sql = connection.createStatement(); 
		} catch (SQLException e) {
		System.out.println("create error"); 
		return false;	
	}
	
    String sqlText = "CREATE Table a2.mostPopulousCountries("
        + "cid INTEGER,"
        + "cname VARCHAR(20)"
        + ")";
    
    try {
        sql.executeUpdate(sqlText);
        if (sql.getUpdateCount() < 0) {
            return false;
        }

        
        sqlText = "INSERT INTO a2.mostPopulousCountries( " + 
                "SELECT cid, cname " + 
                "FROM a2.country " + 
                "WHERE population > 100e6 ORDER BY cid ASC)";

        sql.executeUpdate(sqlText);
        if (sql.getUpdateCount() < 0) {
                return false;
            }
        
        return true;
  
  } catch (SQLException e) {
            System.out.println("updateDB error");
            return false;
    }
  
   }
}
