import mysql.connector
from mysql.connector import Error
import sqlite3


try:
    mydb = mysql.connector.connect(
        host='localhost',
        database='European_Soccer',
        user='root',
        password='12345678'
    )
    if mydb.is_connected():
        db_Info = mydb.get_server_info()
        mydb.start_transaction()
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

# # Create League Table
sql_stmt = """CREATE TABLE IF NOT EXISTS `League` (
    `id` INTEGER PRIMARY KEY AUTO_INCREMENT,
    `country_id` INTEGER,
    `name` VARCHAR(255) UNIQUE,
    FOREIGN KEY(`country_id`) REFERENCES `Country`(`id`)
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
        gk_reflexes
    )
    VALUES (%s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s
    )
    """
    player_attributes_data = list(sqlite_cursor.execute("SELECT * FROM Player_Attributes"))
    size = len(player_attributes_data)
    eu_soccer_cursor.executemany(sql_stmt, list(sqlite_cursor.execute("SELECT * FROM Player_Attributes")))

# Create Team Table
sql_stmt = """CREATE TABLE IF NOT EXISTS `Team` (
    `id` INTEGER PRIMARY KEY AUTO_INCREMENT,
    `team_api_id` INTEGER UNIQUE,
    `team_fifa_api_id` INTEGER UNIQUE,
    `team_long_name` TEXT,
    `team_short_name` TEXT
)"""
eu_soccer_cursor.execute(sql_stmt)
eu_soccer_cursor.execute("SELECT COUNT(*) from Team")
row_numbers, = eu_soccer_cursor.fetchone()
if not row_numbers:
    sql_stmt = """
    INSERT INTO Team (
        id,
        team_api_id,
        team_fifa_api_id,
        team_long_name,
        team_short_name
    )
    VALUES (%s, %s, %s, %s, %s)
        """
    eu_soccer_cursor.executemany(sql_stmt, list(sqlite_cursor.execute("SELECT * FROM Team")))

# Create Team_Attributes Table
sql_stmt = """CREATE TABLE IF NOT EXISTS `Team_Attributes` (
    `id` INTEGER PRIMARY KEY AUTO_INCREMENT,
    `team_fifa_api_id` INTEGER,
    `team_api_id` INTEGER,
    `date` TEXT,
    `buildUpPlaySpeed` INTEGER,
    `buildUpPlaySpeedClass` TEXT,
    `buildUpPlayDribbling` INTEGER,
    `buildUpPlayDribblingClass` TEXT,
    `buildUpPlayPassing` INTEGER,
    `buildUpPlayPassingClass` TEXT,
    `buildUpPlayPositioningClass` TEXT,
    `chanceCreationPassing` INTEGER,
    `chanceCreationPassingClass` TEXT,
    `chanceCreationCrossing` INTEGER,
    `chanceCreationCrossingClass` TEXT,
    `chanceCreationShooting` INTEGER,
    `chanceCreationShootingClass` TEXT,
    `chanceCreationPositioningClass` TEXT,
    `defencePressure` INTEGER,
    `defencePressureClass` TEXT,
    `defenceAggression` INTEGER,
    `defenceAggressionClass` TEXT,
    `defenceTeamWidth` INTEGER,
    `defenceTeamWidthClass` TEXT,
    `defenceDefenderLineClass` TEXT,
    FOREIGN KEY(`team_api_id`) REFERENCES `Team`(`team_api_id`)
)"""
eu_soccer_cursor.execute(sql_stmt)
eu_soccer_cursor.execute("SELECT COUNT(*) from Team_Attributes")
row_numbers, = eu_soccer_cursor.fetchone()
if not row_numbers:
    sql_stmt = """
    INSERT INTO Team_Attributes (
        id,
        team_fifa_api_id,
        team_api_id,
        date,
        buildUpPlaySpeed,
        buildUpPlaySpeedClass,
        buildUpPlayDribbling,
        buildUpPlayDribblingClass,
        buildUpPlayPassing,
        buildUpPlayPassingClass,
        buildUpPlayPositioningClass,
        chanceCreationPassing,
        chanceCreationPassingClass,
        chanceCreationCrossing,
        chanceCreationCrossingClass,
        chanceCreationShooting,
        chanceCreationShootingClass,
        chanceCreationPositioningClass,
        defencePressure,
        defencePressureClass,
        defenceAggression,
        defenceAggressionClass,
        defenceTeamWidth,
        defenceTeamWidthClass,
        defenceDefenderLineClass
    )
    VALUES (%s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
    )
    """
    eu_soccer_cursor.executemany(sql_stmt, list(sqlite_cursor.execute("SELECT * FROM Team_Attributes")))

#Create Match Table
sql_stmt = """
CREATE TABLE IF NOT EXISTS `Match` (
    `id` INTEGER PRIMARY KEY AUTO_INCREMENT,
    `country_id` INTEGER,
    `league_id` INTEGER,
    `season` TEXT,
    `stage` INTEGER,
    `date` TEXT,
    `match_api_id` INTEGER UNIQUE,
    `home_team_api_id` INTEGER,
    `away_team_api_id` INTEGER,
    `home_team_goal` INTEGER,
    `away_team_goal` INTEGER,
    `home_player_X1` INTEGER,
    `home_player_X2` INTEGER,
    `home_player_X3` INTEGER,
    `home_player_X4` INTEGER,
    `home_player_X5` INTEGER,
    `home_player_X6` INTEGER,
    `home_player_X7` INTEGER,
    `home_player_X8` INTEGER,
    `home_player_X9` INTEGER,
    `home_player_X10` INTEGER,
    `home_player_X11` INTEGER,
    `away_player_X1` INTEGER,
    `away_player_X2` INTEGER,
    `away_player_X3` INTEGER,
    `away_player_X4` INTEGER,
    `away_player_X5` INTEGER,
    `away_player_X6` INTEGER,
    `away_player_X7` INTEGER,
    `away_player_X8` INTEGER,
    `away_player_X9` INTEGER,
    `away_player_X10` INTEGER,
    `away_player_X11` INTEGER,
    `home_player_Y1` INTEGER,
    `home_player_Y2` INTEGER,
    `home_player_Y3` INTEGER,
    `home_player_Y4` INTEGER,
    `home_player_Y5` INTEGER,
    `home_player_Y6` INTEGER,
    `home_player_Y7` INTEGER,
    `home_player_Y8` INTEGER,
    `home_player_Y9` INTEGER,
    `home_player_Y10` INTEGER,
    `home_player_Y11` INTEGER,
    `away_player_Y1` INTEGER,
    `away_player_Y2` INTEGER,
    `away_player_Y3` INTEGER,
    `away_player_Y4` INTEGER,
    `away_player_Y5` INTEGER,
    `away_player_Y6` INTEGER,
    `away_player_Y7` INTEGER,
    `away_player_Y8` INTEGER,
    `away_player_Y9` INTEGER,
    `away_player_Y10` INTEGER,
    `away_player_Y11` INTEGER,
    `home_player_1` INTEGER,
    `home_player_2` INTEGER,
    `home_player_3` INTEGER,
    `home_player_4` INTEGER,
    `home_player_5` INTEGER,
    `home_player_6` INTEGER,
    `home_player_7` INTEGER,
    `home_player_8` INTEGER,
    `home_player_9` INTEGER,
    `home_player_10` INTEGER,
    `home_player_11` INTEGER,
    `away_player_1` INTEGER,
    `away_player_2` INTEGER,
    `away_player_3` INTEGER,
    `away_player_4` INTEGER,
    `away_player_5` INTEGER,
    `away_player_6` INTEGER,
    `away_player_7` INTEGER,
    `away_player_8` INTEGER,
    `away_player_9` INTEGER,
    `away_player_10` INTEGER,
    `away_player_11` INTEGER,
    `goal` TEXT,
    `shoton` TEXT,
    `shotoff` TEXT,
    `foulcommit` TEXT,
    `card` TEXT,
    `cross` TEXT,
    `corner` TEXT,
    `possession` TEXT,
    `B365H` NUMERIC,
    `B365D` NUMERIC,
    `B365A` NUMERIC,
    `BWH` NUMERIC,
    `BWD` NUMERIC,
    `BWA` NUMERIC,
    `IWH` NUMERIC,
    `IWD` NUMERIC,
    `IWA` NUMERIC,
    `LBH` NUMERIC,
    `LBD` NUMERIC,
    `LBA` NUMERIC,
    `PSH` NUMERIC,
    `PSD` NUMERIC,
    `PSA` NUMERIC,
    `WHH` NUMERIC,
    `WHD` NUMERIC,
    `WHA` NUMERIC,
    `SJH` NUMERIC,
    `SJD` NUMERIC,
    `SJA` NUMERIC,
    `VCH` NUMERIC,
    `VCD` NUMERIC,
    `VCA` NUMERIC,
    `GBH` NUMERIC,
    `GBD` NUMERIC,
    `GBA` NUMERIC,
    `BSH` NUMERIC,
    `BSD` NUMERIC,
    `BSA` NUMERIC,
    FOREIGN KEY(`country_id`) REFERENCES `Country`(`id`),
    FOREIGN KEY(`league_id`) REFERENCES `League`(`id`),
    FOREIGN KEY(`home_team_api_id`) REFERENCES `Team`(`team_api_id`),
    FOREIGN KEY(`away_team_api_id`) REFERENCES `Team`(`team_api_id`),
    FOREIGN KEY(`home_player_1`) REFERENCES `Player`(`player_api_id`),
    FOREIGN KEY(`home_player_2`) REFERENCES `Player`(`player_api_id`),
    FOREIGN KEY(`home_player_3`) REFERENCES `Player`(`player_api_id`),
    FOREIGN KEY(`home_player_4`) REFERENCES `Player`(`player_api_id`),
    FOREIGN KEY(`home_player_5`) REFERENCES `Player`(`player_api_id`),
    FOREIGN KEY(`home_player_6`) REFERENCES `Player`(`player_api_id`),
    FOREIGN KEY(`home_player_7`) REFERENCES `Player`(`player_api_id`),
    FOREIGN KEY(`home_player_8`) REFERENCES `Player`(`player_api_id`),
    FOREIGN KEY(`home_player_9`) REFERENCES `Player`(`player_api_id`),
    FOREIGN KEY(`home_player_10`) REFERENCES `Player`(`player_api_id`),
    FOREIGN KEY(`home_player_11`) REFERENCES `Player`(`player_api_id`),
    FOREIGN KEY(`away_player_1`) REFERENCES `Player`(`player_api_id`),
    FOREIGN KEY(`away_player_2`) REFERENCES `Player`(`player_api_id`),
    FOREIGN KEY(`away_player_3`) REFERENCES `Player`(`player_api_id`),
    FOREIGN KEY(`away_player_4`) REFERENCES `Player`(`player_api_id`),
    FOREIGN KEY(`away_player_5`) REFERENCES `Player`(`player_api_id`),
    FOREIGN KEY(`away_player_6`) REFERENCES `Player`(`player_api_id`),
    FOREIGN KEY(`away_player_7`) REFERENCES `Player`(`player_api_id`),
    FOREIGN KEY(`away_player_8`) REFERENCES `Player`(`player_api_id`),
    FOREIGN KEY(`away_player_9`) REFERENCES `Player`(`player_api_id`),
    FOREIGN KEY(`away_player_10`) REFERENCES `Player`(`player_api_id`),
    FOREIGN KEY(`away_player_11`) REFERENCES `Player`(`player_api_id`)
)"""
eu_soccer_cursor.execute(sql_stmt)
eu_soccer_cursor.execute("SELECT COUNT(*) FROM `Match`")
row_numbers, = eu_soccer_cursor.fetchone()
if not row_numbers:
    sql_stmt = """
    INSERT INTO `Match` (
        `id`,
        `country_id`,
        `league_id`,
        `season`,
        `stage`,
        `date`,
        `match_api_id`,
        `home_team_api_id`,
        `away_team_api_id`,
        `home_team_goal`,
        `away_team_goal`,
        `home_player_X1`,
        `home_player_X2`,
        `home_player_X3`,
        `home_player_X4`,
        `home_player_X5`,
        `home_player_X6`,
        `home_player_X7`,
        `home_player_X8`,
        `home_player_X9`,
        `home_player_X10`,
        `home_player_X11`,
        `away_player_X1`,
        `away_player_X2`,
        `away_player_X3`,
        `away_player_X4`,
        `away_player_X5`,
        `away_player_X6`,
        `away_player_X7`,
        `away_player_X8`,
        `away_player_X9`,
        `away_player_X10`,
        `away_player_X11`,
        `home_player_Y1`,
        `home_player_Y2`,
        `home_player_Y3`,
        `home_player_Y4`,
        `home_player_Y5`,
        `home_player_Y6`,
        `home_player_Y7`,
        `home_player_Y8`,
        `home_player_Y9`,
        `home_player_Y10`,
        `home_player_Y11`,
        `away_player_Y1`,
        `away_player_Y2`,
        `away_player_Y3`,
        `away_player_Y4`,
        `away_player_Y5`,
        `away_player_Y6`,
        `away_player_Y7`,
        `away_player_Y8`,
        `away_player_Y9`,
        `away_player_Y10`,
        `away_player_Y11`,
        `home_player_1`,
        `home_player_2`,
        `home_player_3`,
        `home_player_4`,
        `home_player_5`,
        `home_player_6`,
        `home_player_7`,
        `home_player_8`,
        `home_player_9`,
        `home_player_10`,
        `home_player_11`,
        `away_player_1`,
        `away_player_2`,
        `away_player_3`,
        `away_player_4`,
        `away_player_5`,
        `away_player_6`,
        `away_player_7`,
        `away_player_8`,
        `away_player_9`,
        `away_player_10`,
        `away_player_11`,
        `goal`,
        `shoton`,
        `shotoff`,
        `foulcommit`,
        `card`,
        `cross`,
        `corner`,
        `possession`,
        `B365H`,
        `B365D`,
        `B365A`,
        `BWH`,
        `BWD`,
        `BWA`,
        `IWH`,
        `IWD`,
        `IWA`,
        `LBH`,
        `LBD`,
        `LBA`,
        `PSH`,
        `PSD`,
        `PSA`,
        `WHH`,
        `WHD`,
        `WHA`,
        `SJH`,
        `SJD`,
        `SJA`,
        `VCH`,
        `VCD`,
        `VCA`,
        `GBH`,
        `GBD`,
        `GBA`,
        `BSH`,
        `BSD`,
        `BSA`
    )
    VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s
    )"""
    eu_soccer_cursor.executemany(sql_stmt, list(sqlite_cursor.execute("SELECT * FROM Match")))
mydb.commit()

