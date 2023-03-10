import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import mysql.connector
from mysql.connector import Error


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

print("European_Soccer tables >>>>> ")
tables = pd.read_sql("SHOW TABLES;", mydb)
print(tables)

print("Country Table >>>>> ")
tables = pd.read_sql("SELECT * FROM Country", mydb)
print(tables)

print("List of League >>>>>>>>>>> ")
leagues = pd.read_sql("""SELECT *
                        FROM League
                        JOIN Country ON Country.id = League.country_id;""", mydb)
print(leagues)
#
print("List of Team >>>>>>>>>>>>")
teams = pd.read_sql("""SELECT *
                        FROM Team
                        ORDER BY team_long_name
                        LIMIT 10;""", mydb)
print(teams)
#
print("List of Match >>>>>>>>>>>")
detailed_matches = pd.read_sql("""SELECT `Match`.id,
                                        Country.name,
                                        League.name,
                                        season,
                                        stage,
                                        date,
                                        HT.team_long_name AS  home_team,
                                        AT.team_long_name AS away_team,
                                        home_team_goal,
                                        away_team_goal
                                FROM `Match`
                                JOIN Country on Country.id = `Match`.country_id
                                JOIN League on League.id = `Match`.league_id
                                LEFT JOIN Team AS HT on HT.team_api_id = `Match`.home_team_api_id
                                LEFT JOIN Team AS AT on AT.team_api_id = `Match`.away_team_api_id
                                WHERE Country.name = 'Spain'
                                ORDER by date
                                LIMIT 10;""", mydb)
print(detailed_matches)
#
print("League by season >>>>>>>>>>>>>>>> ")
leagues_by_season = pd.read_sql("""SELECT Country.name as country_name,
                                        League.name as league_name,
                                        season,
                                        count(distinct stage) AS number_of_stages,
                                        count(distinct HT.team_long_name) AS number_of_teams,
                                        avg(home_team_goal) AS avg_home_team_scors,
                                        avg(away_team_goal) AS avg_away_team_goals,
                                        avg(home_team_goal-away_team_goal) AS avg_goal_dif,
                                        avg(home_team_goal+away_team_goal) AS avg_goals,
                                        sum(home_team_goal+away_team_goal) AS total_goals
                                FROM `Match`
                                JOIN Country on Country.id = `Match`.country_id
                                JOIN League on League.id = `Match`.league_id
                                LEFT JOIN Team AS HT on HT.team_api_id = `Match`.home_team_api_id
                                LEFT JOIN Team AS AT on AT.team_api_id = `Match`.away_team_api_id
                                WHERE Country.name in ('Spain', 'Germany', 'France', 'Italy', 'England')
                                GROUP BY Country.name, League.name, season
                                HAVING count(distinct stage) > 10
                                ORDER BY Country.name, League.name, season DESC
                                ;""", mydb)
print(leagues_by_season)
#
df = pd.DataFrame(index=np.sort(leagues_by_season['season'].unique()), columns=leagues_by_season['country_name'].unique())
df.loc[:,'Germany'] = list(leagues_by_season.loc[leagues_by_season['country_name']=='Germany','avg_goals'])
df.loc[:,'Spain']   = list(leagues_by_season.loc[leagues_by_season['country_name']=='Spain','avg_goals'])
df.loc[:,'France']   = list(leagues_by_season.loc[leagues_by_season['country_name']=='France','avg_goals'])
df.loc[:,'Italy']   = list(leagues_by_season.loc[leagues_by_season['country_name']=='Italy','avg_goals'])
df.loc[:,'England']   = list(leagues_by_season.loc[leagues_by_season['country_name']=='England','avg_goals'])
df.plot(figsize=(12,5),title='Average Goals per Game Over Time')
plt.xlabel("Times")
plt.ylabel("average goals")
plt.show()

df = pd.DataFrame(index=np.sort(leagues_by_season['season'].unique()), columns=leagues_by_season['country_name'].unique())
df.loc[:,'Germany'] = list(leagues_by_season.loc[leagues_by_season['country_name']=='Germany','avg_goal_dif'])
df.loc[:,'Spain']   = list(leagues_by_season.loc[leagues_by_season['country_name']=='Spain','avg_goal_dif'])
df.loc[:,'France']   = list(leagues_by_season.loc[leagues_by_season['country_name']=='France','avg_goal_dif'])
df.loc[:,'Italy']   = list(leagues_by_season.loc[leagues_by_season['country_name']=='Italy','avg_goal_dif'])
df.loc[:,'England']   = list(leagues_by_season.loc[leagues_by_season['country_name']=='England','avg_goal_dif'])
df.plot(figsize=(12,5),title='Average Goals Diff per Game Over Time')
plt.xlabel("Times")
plt.ylabel("avg_goal_dif")
plt.show()

df = pd.DataFrame(index=np.sort(leagues_by_season['season'].unique()), columns=leagues_by_season['country_name'].unique())
df.loc[:,'Germany'] = list(leagues_by_season.loc[leagues_by_season['country_name']=='Germany','total_goals'])
df.loc[:,'Spain']   = list(leagues_by_season.loc[leagues_by_season['country_name']=='Spain','total_goals'])
df.loc[:,'France']   = list(leagues_by_season.loc[leagues_by_season['country_name']=='France','total_goals'])
df.loc[:,'Italy']   = list(leagues_by_season.loc[leagues_by_season['country_name']=='Italy','total_goals'])
df.loc[:,'England']   = list(leagues_by_season.loc[leagues_by_season['country_name']=='England','total_goals'])
df.plot(figsize=(12,5),title='Total Goals per Game Over Time')

plt.xlabel("Times")
plt.ylabel("total_goals")
plt.show()
