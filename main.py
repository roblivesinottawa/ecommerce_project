from faulthandler import cancel_dump_traceback_later
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
    
    def get_utm_content(self):
        '''gets the utm_content and sessions'''
        try:
            conn = self.make_connection()
            cursor = conn.cursor()
            cursor.execute('''
            SELECT 
                ws.utm_content, 
                COUNT(DISTINCT ws.website_session_id) AS sessions ,
                COUNT(DISTINCT o.order_id) AS orders,
                COUNT(DISTINCT o.order_id) / COUNT(DISTINCT ws.website_session_id) AS session_to_order_conv_rt
            FROM website_sessions ws
                LEFT JOIN orders o 
                    ON o.website_session_id = ws.website_session_id
            WHERE ws.website_session_id BETWEEN 1000 AND 2000 
            GROUP BY 1 
            ORDER BY 2 DESC;''')
            [print(row) for row in cursor]
            cursor.close()
            conn.close()
        except Error as e:
            print(e)

    def site_traffic_breakdown(self):
        '''displays a breakdown of site traffic by utm source, campaign, and referring domain'''
        try:
            conn = self.make_connection()
            cursor = conn.cursor()
            cursor.execute('''
            SELECT
                utm_source, utm_campaign, http_referer,
                COUNT(DISTINCT website_session_id) AS sessions
            FROM website_sessions
            WHERE created_at < '2012-04-12'
            GROUP BY 1, 2, 3
            ORDER BY 4 DESC;

          ''')
            [print(row) for row in cursor]
            cursor.close()
            conn.close()
        except Error as e:
            print(e)


    def calculate_conversion_rate(self):
        '''displays a breakdown of site traffic by utm source, campaign, and referring domain'''
        try:
            conn = self.make_connection()
            cursor = conn.cursor()
            cursor.execute('''
            select 
                count(distinct ws.website_session_id) AS sessions, 
                count(distinct o.order_id) as orders,
                count(distinct ws.website_session_id) / count(distinct o.order_id) as session_to_order_conv_rt
            from 
                website_sessions ws
            left join 
                orders o 
            on 
                o.website_session_id = ws.website_session_id 
            where ws.created_at < '2012-04-14' 
            and utm_source = 'gsearch' 
            and utm_campaign = 'nonbrand';

          ''')
            [print(row) for row in cursor]
            cursor.close()
            conn.close()
        except Error as e:
            print(e)

maven = MavenFuzzyFactory('root', input('enter your password: '), 'localhost', 'mavenfuzzyfactory')
# print(maven.show_database())
# print(maven.show_all_tables())
# print(maven.get_utm_content())
print(maven.calculate_conversion_rate())