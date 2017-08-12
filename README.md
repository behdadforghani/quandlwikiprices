# quandlwikiprices

This software downloads end of the day stock prices from Quandl WIKI database, extracts and stores
the updates using Apache Spark into a local database.

## building
1. Install maven
2. Type:
	mvn clean compile package
	
## Setup
To use this software, first set the following environment variables:


````
export QUANDL_KEY="yourkey"
export DB_USER="db user"
export DB_PASS="db password"
export JDBC_URL="jdbc:postgresql://localhost:5432/postgres"
````