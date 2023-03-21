# import necessary libraries
import airflow 
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator

# define DAG
mlb_dag=DAG(
    dag_id="baseball_info_v2",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval=None,
)

# define tasks
def _get_team():
    # request team data
    response = requests.get("http://lookup-service-prod.mlb.com/json/named.team_all_season.bam?sports_code='mlb'&season='2021'")
    teams = response.json()['team_all_season']['queryResults']['row']
    print(f"MLB lookup service API response code for team data: {response.status_code}")

    # filter down to Blue Jays
    team = list(x['team_id'] for x in teams if x['name_display_full'] == "Toronto Blue Jays")
    return team[0]

def _get_players(**context):
    # fetch team value from team task
    team_id = context['task_instance'].xcom_pull(task_ids='get_team')

    # get player data from API
    response = requests.get(f"https://lookup-service-prod.mlb.com/json/named.roster_40.bam?team_id='{team_id}'")
    players = response.json()['roster_40']['queryResults']['row']
    print(f"MLB lookup service API response code for players data: {response.status_code}")

    # filter to left-handed batters
    return list(x for x in players if x['bats'] == 'L')

def _get_games(**context):
     # fetch team value from team task
    team_id = context['task_instance'].xcom_pull(task_ids='get_team')

    # get player data from API
    response = requests.get(f"https://lookup-service-prod.mlb.com/json/named.mlb_broadcast_info.bam?sort_by='game_time_et_asc'&season='2021'")
    broadcasts = response.json()['mlb_broadcast_info']['queryResults']['row']
    print(f"MLB lookup service API response code for games data: {response.status_code}")

    # filter to team-specified games
    team_broadcasts = list(x for x in broadcasts if x['home_team_id'] == team_id)

    games = []
    for i in range(10):
        games.append(team_broadcasts[i]['away_team_full'])
    
    return games

def _write_file(**context):
    # get data from game and player tasks
    left_handed_players = context['task_instance'].xcom_pull(task_ids='get_players')
    home_games = context['task_instance'].xcom_pull(task_ids='get_games')

    # write to file
    with open('/Users/nishantsulay/airflow/dags/left_handed_results.txt', 'w') as f:
        f.write("Left-handed players:\n--------------------\n")

        for player in left_handed_players:
            f.write(player['name_display_first_last'] + "\n")
    
        f.write("\n\nOpponents in first five home games:\n--------------------\n")
        for game in home_games:
            f.write(game + "\n")

# write operators
get_team = PythonOperator(
    task_id = "get_team",
    python_callable=_get_team,
    dag=mlb_dag,
)

get_players = PythonOperator(
    task_id = "get_players",
    python_callable=_get_players,
    provide_context=True,
    dag=mlb_dag,
)

get_games = PythonOperator(
    task_id = "get_games",
    python_callable=_get_games,
    provide_context=True,
    dag=mlb_dag,
)

write_file = PythonOperator(
    task_id = "write_file",
    python_callable=_write_file,
    provide_context=True,
    dag=mlb_dag,
)

# structure dag
get_team >> [get_players, get_games] >> write_file