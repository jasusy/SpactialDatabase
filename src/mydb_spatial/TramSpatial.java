package mydb_spatial;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class TramSpatial {
	static final String JDBC_DRIVER ="com.mysql.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://localhost:3306/my_db";
	static final String DB_USER = "root";
	static final String DB_PASS = "9147";

	/**
	 * Entrance of spatial database program
	 * Run by following 4 type of formats: java TramSpatial [type] [object] [parameters]:
	 * type = window: java TramSpatial window [object] [x1-coo] [y1-coo] [x2-coo] [y2-coo]
	 * In above command point1 is left bottom-point point2 is right-top point
	 * type = within: java TramSpatial within [student id] [distance]
	 * display all objects having in distance from target student
	 * type = nearest-neighbor: java TramSpatial nearest-neighbor [object] [building id] [number]
	 * display given number of objects of nearest neighbor(s) for target building
	 * type = fixed : java TramSpatial fixed [query number]
	 * Run fixed queries pre-loaded on screen
	 * Objects are: "building", "student" and "tramstop"
	 * Command is case insensitive
	 * @param args
	 */
	public static void main(String[] args) {

		if(args.length < 2 || args.length > 6) {
			System.out.println("Please input valid command");
			System.exit(0);
		}

		//1. when command type is "window": window [object] [x1-coo] [y1-coo] [x2-coo] [y2-coo]
		String ctype = args[0].toLowerCase();
		if(ctype.equals("window")){
			if(args.length != 6) {
				System.out.println("Input window [object] [x1-coo] [y1-coo] [x2-coo] [y2-coo]");
				System.exit(0);
			}
			String obj = args[1].toLowerCase();
			if(!obj.equals("student") && !obj.equals("building") && !obj.equals("tramstop")){
				System.out.println("Input window [object] [x1-coo] [y1-coo] [x2-coo] [y2-coo]");
				System.exit(0);
			}
			try{
				int x1 = Integer.parseInt(args[2]);
				int y1 = Integer.parseInt(args[3]);
				int x2 = Integer.parseInt(args[4]);
				int y2 = Integer.parseInt(args[5]);
				queryWindow(obj, x1, y1, x2, y2);
			} catch (NumberFormatException nfe) {
				nfe.printStackTrace();
				System.out.println("Input window [object] [x1-coo] [y1-coo] [x2-coo] [y2-coo]");
				System.exit(0);
			}
		}
		//2. when command type is "within": within [student id] [distance]
		else if(ctype.equals("within")){
			if(args.length != 3) {
				System.out.println("Input within [student id] [distance]");
				System.exit(0);				
			}
			try{
				int num = Integer.parseInt(args[2]);
				queryWithin(args[1], num);
			} catch (NumberFormatException nfe) {
				nfe.printStackTrace();
				System.out.println("Input within [student id] [distance]");
				System.exit(0);
			}

		}
		//3. when command type is "nearest-neighbor": nearest-neighbor [building id] [number]
		else if(ctype.equals("nearest-neighbor")){
			if(args.length != 4) {
				System.out.println("Input nearest-neighbor [object] [building id] [number]");
				System.exit(0);				
			}
			String obj = args[1].toLowerCase();
			if(!obj.equals("student") && !obj.equals("building") && !obj.equals("tramstop")){
				System.out.println("Input nearest-neighbor [object] [building id] [number]");
				System.exit(0);
			}
			try{
				int num = Integer.parseInt(args[3]);
				queryNeighbor(obj, args[2], num);
			} catch (NumberFormatException nfe) {
				nfe.printStackTrace();
				System.out.println("Input nearest-neighbor [object] [building id] [number]");
				System.exit(0);
			}

		}
		//4. when command type is "fixed": fixed [query number]
		else if(ctype.equals("fixed")){
			if(args.length != 2) {
				System.out.println("Input fixed [query number]");
				System.exit(0);				
			}
			try{
				int num = Integer.parseInt(args[1]);
				queryFixed(num);
			} catch (NumberFormatException nfe) {
				nfe.printStackTrace();
				System.out.println("Input fixed [query number]");
				System.exit(0);
			}
		}
		//5. Otherwise, input invalid
		else {
			System.out.println("Invalid command!");
			System.exit(0);				
		}

		System.out.println("End of program.");
	}

	/**
	 * Processing "window" type command
	 * @param obj
	 * @param x1
	 * @param y1
	 * @param x2
	 * @param y2
	 */
	private static void queryWindow(String obj, int x1, int y1, int x2, int y2){
		Connection conn = null;
		Statement st = null;

		try{
			Class.forName(JDBC_DRIVER);
			System.out.println("Connecting to database ...");
			conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);

			System.out.println("Creating statement ...");
			st = conn.createStatement();
			System.out.println(obj + " " + x1 + " " + y1 + " " + x2 + " " + y2);
			//			String sql = "SELECT id, AsText(location) from " + obj
			//					+ "s WHERE ST_X(location) > " + x1 + " && ST_X(location) < " + x2
			//					+ " && ST_Y(location) > " + y1 + " && ST_Y(location) < " + y2 ;
			StringBuilder sb = new StringBuilder();
			sb.append("SELECT id, AsText(location) from " + obj
					+ "s WHERE ST_Contains(Envelope(GeomFromText('LineString("
					+ x1 + " " + y1 + ", " 	+ x2 + " " + y2 + ")')), location)");
			ResultSet  rs = st.executeQuery(sb.toString());

			while(rs.next()){
				String id = rs.getString("id");
				String loc;
				loc = rs.getString(2);
				System.out.println("ID: " + id + ", loc: " + loc);
			}

			rs.close();
			st.close();
			conn.close();
		} catch(SQLException se){
			se.printStackTrace();
		} catch(Exception e){
			e.printStackTrace();
		}

	}

	/**
	 * Processing "within" type command
	 * @param studentID
	 * @param num
	 */
	private static void queryWithin(String studentID, int dis) {
		Connection conn = null;
		Statement st = null;
		try{
			Class.forName(JDBC_DRIVER);
			System.out.println("Connecting to database ...");
			conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
			System.out.println("Creating statement ...");
			st = conn.createStatement();
			//			System.out.println(studentID + " " + dis);

			//query tramstops
			StringBuilder sb = new StringBuilder();
			ResultSet  rs;
			sb.append("SELECT id, AsText(location), radius from tramstops WHERE st_distance(location, "
					+ "(SELECT location from students where id='" + studentID + "')) < " + dis);
			//			System.out.println(sb.toString());
			rs = st.executeQuery(sb.toString());
			//display result
			while(rs.next()){
				String id = rs.getString("id");
				String loc;
				loc = rs.getString(2);
				int rad = rs.getInt(3);
				System.out.println("ID: " + id + ", loc: " + loc + ", radius: " + rad);
			}
			//query buildings
			sb.setLength(0);
			sb.append("SELECT id, name, vertnum, AsText(location) from buildings WHERE st_distance(location, "
					+ "(SELECT location from students where id='" + studentID + "')) < " + dis);
			//			System.out.println(sb.toString());
			rs = st.executeQuery(sb.toString());
			//display result
			while(rs.next()){
				String id = rs.getString("id");
				String name = rs.getString(2);
				String vertnum = rs.getString(3);
				String loc = rs.getString(4);
				System.out.println("ID: " + id + ", name: " + name + ", vertnum: " + vertnum + ",loc: " + loc);
			}

			rs.close();
			st.close();
			conn.close();
		} catch(SQLException se){
			se.printStackTrace();
		} catch(Exception e){
			e.printStackTrace();
		}


	}	

	/**
	 * Processing "Nearest-Neighbor" command
	 * @param id
	 * @param num
	 */
	private static void queryNeighbor(String obj, String tid, int num) {
		Connection conn = null;
		Statement st = null;
		try{
			//setup connection
			Class.forName(JDBC_DRIVER);
			System.out.println("Connecting to database ...");
			conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
			System.out.println("Creating statement ...");
			st = conn.createStatement();
			System.out.println(obj + " " + tid + " " + num);
			//query data
			StringBuilder sb = new StringBuilder();
			sb.append("SELECT id, astext(location) from "+ obj +"s ORDER BY "
					+ "st_distance(location, (SELECT location from "+ obj +"s where id='"+ tid +"')) LIMIT "+ (num+1) +";");
			ResultSet  rs = st.executeQuery(sb.toString());
			//print result
			rs.next();
			while(rs.next()){
				String id = rs.getString("id");
				String loc;
				loc = rs.getString(2);
				System.out.println("ID: " + id + ", loc: " + loc);
			}
			//close connection and release resource
			rs.close();
			st.close();
			conn.close();
		} catch(SQLException se){
			se.printStackTrace();
		} catch(Exception e){
			e.printStackTrace();
		}

	}

	/**
	 * Processing "Fixed" type command
	 * @param num
	 * 1. Find the ids of all the students and buildings cover by tram stops: t2ohe and t6ssl.
	 * 2. For each student, list the ID¡¯s of the 2 nearest tram stops.
	 * 3. We say a tram stop covers a building if it is within distance 250 to that building.
	 * 	Find the ID¡¯s of the tram stop that cover the most buildings.
	 * 4. We say a student is called a reverse nearest neighbor of a building if it is thatbuilding¡¯s nearest student. 
	 * 	Find the ID¡¯s of the top 5 students that have the most 
	 * 	reverse nearest neighbors together with their number of reverse nearest neighbors.
	 * 5. Find the coordinates of the lower left and upper right vertex of the MBR that fully
	 * 	contains all buildings whose names are of the form ¡¯SS%¡¯. Note that you cannot
	 * 	manually figure out these buildings in your program.
	 */
	private static void queryFixed(int num) {
		Connection conn = null;
		Statement st = null;
		Connection conn2 = null;
		Statement st2 = null;

		try{
			//setup connection
			Class.forName(JDBC_DRIVER);
			System.out.println("Connecting to database ...");
			conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
			System.out.println("Creating statement ...");
			st = conn.createStatement();

			//query data && print result
			StringBuilder sb = new StringBuilder();
			ResultSet rs = null;
			switch(num){
			case 1:{
				System.out.println("query fixed 1");
				sb.append("SELECT id, astext(location) from students "
						+ "where (st_distance(location, (SELECT location from tramstops where id='t2ohe')) < (SELECT radius from tramstops where id='t2ohe'))"
						+ "|| (st_distance(location, (SELECT location from tramstops where id='t6ssl')) < (SELECT radius from tramstops where id='t6ssl'));");
				rs = st.executeQuery(sb.toString());
				while(rs.next()){
					System.out.println("ID: " + rs.getString("id") + ", loc: " + rs.getString(2));
				}
				sb.setLength(0);
				sb.append("SELECT id, astext(location) from buildings "
						+ "where (st_distance(location, (SELECT location from tramstops where id='t2ohe')) < (SELECT radius from tramstops where id='t2ohe'))"
						+ "|| (st_distance(location, (SELECT location from tramstops where id='t6ssl')) < (SELECT radius from tramstops where id='t6ssl'));");
				rs = st.executeQuery(sb.toString());
				while(rs.next()){
					System.out.println("ID: " + rs.getString("id") + ", loc: " + rs.getString(2));
				}
				break;
			}
			case 2:{
				System.out.println("query fixed 2");
				sb.append("SELECT id, AsText(location) from students");
				rs = st.executeQuery(sb.toString());
				
				conn2 = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
				st2 = conn2.createStatement();
				while(rs.next()){
					String cursid = rs.getString(1); //current student id
					String curloc = rs.getString(2);
					sb.setLength(0);
					sb.append("SELECT id, astext(location) from tramstops ORDER BY "
							+ "st_distance(location, GeomFromText('"+ curloc +"')) LIMIT 2;");
//					System.out.println(sb.toString());
					ResultSet rs2 = st2.executeQuery(sb.toString());
					//print result
					System.out.print("Student: " + cursid + "==> ");
					while(rs2.next()){
						System.out.print("ID: " + rs2.getString("id") + ", loc: " + rs2.getString(2) + "  ");
					}
					System.out.println();
					rs2.close();
				}
				st2.close();
				conn2.close();
				
				break;
			}
			case 3:{
				System.out.println("query fixed 3");
				sb.append("SELECT id from tramstops");
				rs = st.executeQuery(sb.toString());
				
				conn2 = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
				st2 = conn2.createStatement();
				List<String> res = new ArrayList<>();
				int maxb = -1;
				ResultSet rs2 = null;
				while(rs.next()){
					String curid = rs.getString(1); //current student id
					sb.setLength(0);
					sb.append("SELECT count(*) from buildings where (st_distance(location, (SELECT location from tramstops where id='"+ curid +"')) < 250)");
					rs2 = st2.executeQuery(sb.toString());
					rs2.next();
					int curnum = rs2.getInt(1);
					if(curnum > maxb) {
						res.clear();
						res.add(curid);
						maxb = curnum;
					}
					else if(curnum == maxb){
						res.add(curid);
					}
				}
				rs2.close();
				st2.close();
				conn2.close();
				System.out.println("tramstop id with most building covered: ");
				for(String str : res){
					System.out.print(str + " ");
				}
				System.out.println(maxb);
				break;
			}
			case 4:{
				System.out.println("query fixed 4");
				rs = st.executeQuery("SELECT COUNT(*) FROM students");
				rs.next();
				int size = rs.getInt(1);
				int[] res = new int[size];
				sb.append("SELECT id from buildings");
				rs = st.executeQuery(sb.toString());				
				conn2 = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
				st2 = conn2.createStatement();
				
				//write result into res
				ResultSet rs2 = null;
				while(rs.next()){
					String curid = rs.getString(1); //current building id
					sb.setLength(0);
					sb.append("select id from students order by st_distance(location, (select location from buildings where id='"+ curid +"')) limit 1");
					rs2 = st2.executeQuery(sb.toString());
					rs2.next();
					String curstu = rs2.getString(1).substring(1);
					res[Integer.parseInt(curstu)]++;
				}
				rs2.close();
				st2.close();
				conn2.close();
				//print result from res array
				System.out.println("Top 5 students: ");
				boolean[] flag = new boolean[size];
				for(int i = 0; i < 5; i++){
					int max = 0;
					int index = 0;
					while(flag[index]) index++;
					for(int j = index; j < size; j++){
						if(!flag[j] && res[j] > max) {
							max = res[j];
							index = j;
							flag[j] = true;
						}
					}
					System.out.print("p" + index + " " + max + ", ");
				}
				System.out.println();
				break;
			}
			case 5:{
				System.out.println("query fixed 5");
				
		    String envelope = "(select Envelope(GeomFromText(concat(concat(\"geometrycollection(\",group_concat(AsText(location))),\")\"))) "
			    	+ "from( select location from buildings where name like 'ss%' ) as ssBuildings)";

		    String lowerLeftPoint = "(PointN((select ExteriorRing(" + envelope + ")),1))";	 // Linestring starts from 1
		    String upperRightPoint = "(PointN((select ExteriorRing(" + envelope + ")),3))";
		    sb.append("(select X(" + lowerLeftPoint + ") as X, Y(" + lowerLeftPoint + ") as Y )");
		   	sb.append("union ");
		   	sb.append("(select X(" + upperRightPoint + "), Y(" + upperRightPoint + ")); ");

				rs = st.executeQuery(sb.toString());
			  ResultSetMetaData rsmd = rs.getMetaData();
		    int columnsNumber = rsmd.getColumnCount();

		    if(!rs.next()) {
		    	System.out.println("Empty result set");
		    }
		    else{
			    rs.beforeFirst(); // reset back if there are rows
			    while (rs.next()) {
			        for (int i = 1; i <= columnsNumber; i++) {
			            if (i > 1) System.out.print(", ");
			            String columnValue = rs.getString(i);
			            System.out.print(columnValue); 
			        }
	        		System.out.println();
	    		}
		    }
				break;
			}
			default:
				System.out.println("Out of fiexed query range.");
				break;
			}

			//close connection and release resource
			rs.close();
			st.close();
			conn.close();
		} catch(SQLException se){
			se.printStackTrace();
		} catch(Exception e){
			e.printStackTrace();
		}

	}

}
