import mysql.connector
from mysql.connector import connect, Error

class MavenFuzzyFactory:
    def __init__(self, user, password, host, database):
        self.user = user
        self.password = password
        self.host = host
        self.database = database

        self.cursor = self.make_connection()
    
    def make_connection(self):
        '''This function establishes a connection to the database'''
        try:
            conn = connect(user=self.user, password=self.password, host=self.host, database=self.database)
            ('CONNECTION FAILED' if conn.is_connected() == False else 'CONNECTION SUCCESSFULLY ESTABLISHED')
            return conn
        except Error as e:
            print(e)

    def show_database(self):
        '''This function shows the available databases'''
        try:
            conn = self.make_connection()
            cursor = conn.cursor()
            cursor.execute("SHOW DATABASES;")
            [print(db) for db in cursor]
            cursor.close()
            conn.close()
        except Error as e:
            print(e)

    def show_all_tables(self):
        '''This function displays the tables'''
        try:
            conn = self.make_connection()
            cursor = conn.cursor()
            cursor.execute("USE mavenfuzzyfactory;")
            print("DATABASE 'mavenfuzzyfactory' SELECTED SUCCESSFULLY.")
            cursor.execute("SHOW TABLES;")
            [print(table) for table in cursor]
            cursor.close()
            conn.close()
        except Error as e:
            print(e)


maven = MavenFuzzyFactory('root', 'mysqlpassmacrob', 'localhost', 'mavenfuzzyfactory')
# print(maven.show_database())
print(maven.show_all_tables())