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
    
    def trended_session_volume(self):
        '''pulls gsearch  nonbranded trended session volume by week'''
        try:
            conn = self.make_connection()
            cursor = conn.cursor()
            cursor.execute('''
            SELECT 
                MIN(DATE(created_at)) As week_started_at,
                COUNT(DISTINCT website_session_id) AS sessions
            FROM website_sessions
            WHERE created_at < '2012-05-10'
            AND UTM_SOURCE = 'gsearch'
            AND utm_campaign = 'nonbrand'
            GROUP BY
                YEAR(created_at), WEEK(created_at);
            ''')
            [print(row) for row in cursor]
            cursor.close()
            conn.close()
        except Error as e:
            print(e)

    def conversion_rate_by_device_type(self):
        '''pulls the conversion rate from session to order by device type'''
        try:
            conn = self.make_connection()
            cursor = conn.cursor()
            cursor.execute('''
            SELECT
                ws.device_type,
                COUNT(DISTINCT ws.website_session_id) AS sessions,
                COUNT(DISTINCT o.order_id) AS orders,
                COUNT(DISTINCT o.order_id) / COUNT(DISTINCT ws.website_session_id) AS conversion_rate
            FROM
                website_sessions ws
            LEFT JOIN 
                orders o
            ON
                o.website_session_id = ws.website_session_id
            WHERE 
                ws.created_at < '2012-05-11'
            AND utm_source = 'gsearch'
            AND utm_campaign = 'nonbrand'
            GROUP BY 1;
            ''')
            [print(row) for row in cursor]
            cursor.close()
            conn.close()
        except Error as e:
            print(e)

    def mobile_desktop_weekly_trends(self):
        '''pulls weekly trends for both desktop and mobile to disply impact on volume'''
        try:
            conn = self.make_connection()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT
                    MIN(DATE(created_at)) AS week_start_date,
                    COUNT(DISTINCT 
                                CASE WHEN device_type = 'desktop' 
                                THEN website_session_id 
                                ELSE NULL END) as desktop_sessions,
                    COUNT(DISTINCT 
                                CASE WHEN device_type = 'mobile'
                                THEN website_session_id
                                ELSE NULL END) as mobile_sessions
                FROM website_sessions ws
                WHERE ws.created_at < '2012-06-09'
                    AND ws.created_at > '2012-04-15'
                    AND utm_source = 'gsearch'
                    AND utm_campaign = 'nonbrand'
                GROUP BY YEAR(created_at), WEEK(created_at);
            ''')
            [print(row) for row in cursor]
            cursor.close()
            conn.close()
        except Error as e:
            print(e)

    def most_viewed_website_pages(self):
        try:
            conn = self.make_connection()
            cursor = conn.cursor()
            cursor.execute('''
            SELECT wp.pageview_url,
                COUNT(DISTINCT wp.website_pageview_id) AS pageviews
            FROM website_pageviews wp
            WHERE wp.created_at < '2012-06-09'
            GROUP BY 1
            ORDER BY pageviews DESC;''')
            [print(row) for row in cursor]
            cursor.close()
            conn.close()
        except Error as e:
            print(e)
    
    def identify_top_entry_pages(self):
        try:
            conn = self.make_connection()
            cursor = conn.cursor()
            cursor.execute('''
            CREATE TEMPORARY TABLE first_pageview_per_session
            SELECT 
                website_session_id,
                MIN(website_pageview_id) AS first_pageview
            FROM website_pageviews
            WHERE created_at < '2012-06-12'
            GROUP BY website_session_id;
            ''')
            cursor.execute('''
            SELECT 
                wp.pageview_url AS landing_page_url,
                COUNT(DISTINCT fp.website_session_id) AS sessions_hitting_this_landing_page
            FROM first_pageview_per_session fp
                LEFT JOIN 
                    website_pageviews wp
                ON fp.first_pageview = wp.website_pageview_id
            WHERE wp.created_at < '2012-06-12'
            GROUP BY 1
            ORDER BY sessions_hitting_this_landing_page DESC;
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
# print(maven.calculate_conversion_rate())
# print(maven.trended_session_volume())
# print(maven.conversion_rate_by_device_type())
# print(maven.mobile_desktop_weekly_trends())
# print(maven.most_viewed_website_pages())
print(maven.identify_top_entry_pages())