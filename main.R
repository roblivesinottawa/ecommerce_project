library("RMariaDB")

# path
rmariadb.settingsfile <- "main.cnf"
# database name
rmariadb.db <- "mavenfuzzyfactory"
mavenDB <- dbConnect(RMariaDB::MariaDB(), 
                    default.file=rmariadb.settingsfile, 
                    group=rmariadb.db)

# list the tables
print(dbListTables(mavenDB))

# disconnect to clean up connection
print('Disconnectiing from database...')
print(dbDisconnect(mavenDB))