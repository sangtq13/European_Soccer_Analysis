import mysql.connector
from mysql.connector import Error
import sqlite3


try:
    mydb = mysql.connector.connect(host='localhost',
                                         database='European_Soccer',
                                         user='root',
                                         password='12345678')
    if mydb.is_connected():
        db_Info = mydb.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        eu_soccer_cursor = mydb.cursor()
        eu_soccer_cursor.execute("select database();")
        record = eu_soccer_cursor.fetchone()
        print("You're connected to database: ", record)

except Error as e:
    print("Error while connecting to MySQL", e)
finally:
    print("S->13 Session")

conn = sqlite3.connect("database.sqlite")
sqlite_cursor = conn.cursor()

# Create Country Table
sql_stmt = """CREATE TABLE IF NOT EXISTS `Country` (
    `id` INTEGER PRIMARY KEY AUTO_INCREMENT, 
    `name` VARCHAR(255) UNIQUE
)"""
eu_soccer_cursor.execute(sql_stmt)
eu_soccer_cursor.execute("SELECT COUNT(*) from Country")
row_numbers, = eu_soccer_cursor.fetchone()
if not row_numbers:
    sql_stmt = "INSERT INTO Country (id, name) VALUES (%s, %s)"
    eu_soccer_cursor.executemany(sql_stmt, list(sqlite_cursor.execute("SELECT * FROM Country")))

# Create League Table
sql_stmt = """CREATE TABLE IF NOT EXISTS `League` (
    `id` INTEGER PRIMARY KEY AUTO_INCREMENT,
    `country_id` INTEGER, 
    `name` VARCHAR(255) UNIQUE, 
    FOREIGN KEY(`country_id`) REFERENCES `country`(`id`)
)"""
eu_soccer_cursor.execute(sql_stmt)
eu_soccer_cursor.execute("SELECT COUNT(*) from League")
row_numbers, = eu_soccer_cursor.fetchone()
if not row_numbers:
    sql_stmt = "INSERT INTO League (id, country_id, name) VALUES (%s, %s, %s)"
    eu_soccer_cursor.executemany(sql_stmt, list(sqlite_cursor.execute("SELECT * FROM League")))

# Create Player Table
sql_stmt = """CREATE TABLE IF NOT EXISTS `Player` (
    `id` INTEGER PRIMARY KEY AUTO_INCREMENT,
    `player_api_id`	INTEGER UNIQUE,
    `player_name` TEXT,
    `player_fifa_api_id` INTEGER UNIQUE,
    `birthday` TEXT,
    `height` INTEGER,
    `weight` INTEGER
)"""
eu_soccer_cursor.execute(sql_stmt)
eu_soccer_cursor.execute("SELECT COUNT(*) from Player")
row_numbers, = eu_soccer_cursor.fetchone()
if not row_numbers:
    sql_stmt = """INSERT INTO Player (id, player_api_id, player_name, player_fifa_api_id, birthday, height, weight)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    eu_soccer_cursor.executemany(sql_stmt, list(sqlite_cursor.execute("SELECT * FROM Player")))

# Create Player_Attributes Table
sql_stmt = """
CREATE TABLE IF NOT EXISTS `Player_Attributes` (
    `id` INTEGER PRIMARY KEY AUTO_INCREMENT,
    `player_fifa_api_id` INTEGER,
    `player_api_id`	INTEGER,
    `date` TEXT,
    `overall_rating` INTEGER,
    `potential`	INTEGER,
    `preferred_foot` TEXT,
    `attacking_work_rate` TEXT,
    `defensive_work_rate` TEXT,
    `crossing` INTEGER,
    `heading_accuracy` INTEGER,
    `finishing`	INTEGER,
    `short_passing`	INTEGER,
    `volleys` INTEGER,
    `dribbling`	INTEGER,
    `curve`	INTEGER,
    `free_kick_accuracy` INTEGER,
    `long_passing` INTEGER,
    `ball_control` INTEGER,
    `acceleration` INTEGER,
    `sprint_speed` INTEGER,
    `agility` INTEGER,
    `reactions` INTEGER,
    `balance` INTEGER,
    `shot_power` INTEGER,
    `jumping` INTEGER,
    `stamina` INTEGER,
    `strength` INTEGER,
    `long_shots` INTEGER,
    `aggression` INTEGER,
    `interceptions`	INTEGER,
    `positioning` INTEGER,
    `vision` INTEGER,
    `penalties`	INTEGER,
    `marking` INTEGER,
    `standing_tackle` INTEGER,
    `sliding_tackle` INTEGER,
    `gk_diving`	INTEGER,
    `gk_handling` INTEGER,
    `gk_kicking` INTEGER,
    `gk_positioning` INTEGER,
    `gk_reflexes` INTEGER,
    FOREIGN KEY(`player_fifa_api_id`) REFERENCES `Player`(`player_fifa_api_id`),
    FOREIGN KEY(`player_api_id`) REFERENCES `Player`(`player_api_id`)
)"""
eu_soccer_cursor.execute(sql_stmt)
eu_soccer_cursor.execute("SELECT COUNT(*) from Player_Attributes")
row_numbers, = eu_soccer_cursor.fetchone()
if not row_numbers:
    sql_stmt = """
    INSERT INTO Player_Attributes (
        id,
        player_fifa_api_id,
        player_api_id,
        date,
        overall_rating,
        potential,
        preferred_foot,
        attacking_work_rate,
        defensive_work_rate,
        crossing,
        heading_accuracy,
        finishing,
        short_passing,
        volleys,
        dribbling,
        curve,
        free_kick_accuracy,
        long_passing,
        ball_control,
        acceleration,
        sprint_speed,
        agility,
        reactions,
        balance,
        shot_power,
        jumping,
        stamina,
        strength,
        long_shots,
        aggression,
        interceptions,
        positioning,
        vision,
        penalties,
        marking,
        standing_tackle,
        sliding_tackle,
        gk_diving,
        gk_handling,
        gk_kicking,
        gk_positioning,
        gk_reflexes)
    VALUES (%s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s)
    """
    eu_soccer_cursor.execute("SET GLOBAL FOREIGN_KEY_CHECKS=0;")
    eu_soccer_cursor.executemany(sql_stmt, list(sqlite_cursor.execute("SELECT * FROM Player_Attributes")))
    eu_soccer_cursor.execute("SET GLOBAL FOREIGN_KEY_CHECKS=1;")

# Create Team Table
sql_stmt = """CREATE TABLE IF NOT EXISTS `Team` (
    `id` INTEGER PRIMARY KEY AUTO_INCREMENT,
    `team_api_id` INTEGER UNIQUE,
    `team_fifa_api_id` INTEGER,
    `team_long_name` TEXT,
    `team_short_name` TEXT
)"""
eu_soccer_cursor.execute(sql_stmt)
eu_soccer_cursor.execute("SELECT COUNT(*) from Team")
row_numbers, = eu_soccer_cursor.fetchone()
if not row_numbers:
    sql_stmt = """INSERT INTO Team (id, team_api_id, team_fifa_api_id, team_long_name, team_short_name)
            VALUES (%s, %s, %s, %s, %s)
        """
    eu_soccer_cursor.executemany(sql_stmt, list(sqlite_cursor.execute("SELECT * FROM Team")))

mydb.commit()
