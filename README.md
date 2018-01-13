# quandlwikiprices

This software downloads end of the day stock prices from Quandl WIKI database, extracts and stores
the updates using Apache Spark into a local database.

This data set has about 15 million lows and can only be downloaded in its entirety. The job of this
program is to download the data set and upload the updates to a local database.

## building
1. Install maven
2. Type:
````
	mvn clean compile package
````
	
## Setup
To use this software, first set the following environment variables:


````
export QUANDL_KEY="yourkey"
export QUANDL_DB_USER="db user"
export QUANDL_DB_PASS="db password"
export QUANDL_JDBC_URL="jdbc:postgresql://localhost:5432/postgres"
export QUANDL_CHECKPOINT="/path/to/checkpoint/file"
````

Note QUANDL_CHECKPOINT should point to a file where the software will write the date of the last download

## running

The usage of the program is:

````
DownloadEod [start date]
````

Start date causes the software to upload the data after that date. If no start date is specified, the program first checks if the check point file exists. If it does, it uploads the files since the last check point. Otherwise, it will upload the whole database.

### Spark Note
If you do not have a Spark cluster, the following command will run the Spark jobs using 4 threads:

````
java -Dspark.master="local[4]" -Dspark.driver.bindAddress=127.0.0.1 -jar target/download_eod-1.0-SNAPSHOT.jar
````

[![paypal](https://www.paypalobjects.com/en_US/i/btn/btn_donateCC_LG.gif)](https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=MU7T4U52Q4N2N)