package com.igz.performance.database;

public class DatabaseFactory {
	
	public enum DatabaseType {
		MYSQL, MONGODB, REDIS, SQLSERVER
	}

	public static DatabaseDAO createDatabase(DatabaseType type) {
		switch (type) {
		case MYSQL:
			return new MysqlDAO();
		case MONGODB:
			return new MongoDBDAO();
		case REDIS:
			return new RedisDAO();
		case SQLSERVER:
			return new SqlServerDAO();
		default:
			return new MysqlDAO();
		}
		
		
	}
}
