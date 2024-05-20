import pandas as pd
from sqlalchemy import create_engine
from flask import Flask, render_template, request, redirect, url_for
import yaml


app = Flask(__name__)

def get_config(config_path):
    config = yaml.safe_load(open(config_path))

    return config

def calculate_eff(shovel):
    yaml_path="utils/config.yaml"
    yaml_config=get_config(yaml_path)
    yaml_db="localhost"
    host=yaml_config['db'][yaml_db]['host']
    port=yaml_config['db'][yaml_db]['port']
    db=yaml_config['db'][yaml_db]['database']
    user=yaml_config['db'][yaml_db]['user']
    pas=yaml_config['db'][yaml_db]['password']
    
    engine = create_engine(
   f"postgresql://{user}:{pas}@{host}:{ port }/{ db}")

    sql = f"select * from public.operations_trucks"
    df_trucks = pd.read_sql(sql, engine)

    sql = f"select * from public.operations_shovels"
    df_shovels = pd.read_sql(sql, engine)
    
    zone_eff = df_shovels.loc[(df_shovels['operation'] ==  'idle') &(df_shovels['mdm_object_name'] == f'{shovel}'), 'duration'].sum() - df_trucks.loc[(df_trucks['operation'] ==  'idle') &(df_trucks['shov_con'] == f'{shovel}'), 'duration'].sum()
    zone_eff =round(zone_eff/60)
    return zone_eff

@app.route('/')
def my_index():

    yaml_path="utils/config.yaml"
    yaml_config=get_config(yaml_path)
    yaml_db="localhost"
    host=yaml_config['db'][yaml_db]['host']
    port=yaml_config['db'][yaml_db]['port']
    db=yaml_config['db'][yaml_db]['database']
    user=yaml_config['db'][yaml_db]['user']
    pas=yaml_config['db'][yaml_db]['password']
    
    engine = create_engine(
   f"postgresql://{user}:{pas}@{host}:{ port }/{ db}")
    

    messages = {
        'normal':f'оптимальная производительность комплекса',
        'deficit':f'рекомендуем добавить один самосвал в комплекс',
        'proficit':f'рекомендуем убрать один самосвал из комплекса',
    }
    
    sql = f"select * from public.operations_trucks where shov_con = '1'"
    df_trucks_zone1 = pd.read_sql(sql, engine)
    df_trucks_zone1 = df_trucks_zone1['truck_name'].unique()

    sql = f"select * from public.operations_trucks where shov_con = '2'"
    df_trucks_zone2 = pd.read_sql(sql, engine)
    df_trucks_zone2 = df_trucks_zone2['truck_name'].unique()
    
    zone_1_eff = calculate_eff(1)
    zone_2_eff = calculate_eff(2)
    
    if zone_1_eff == 0:
        message_z1 = messages['normal']
    elif zone_1_eff < 0:
        message_z1 = messages['proficit']
    else:
        message_z1 = messages['deficit']

    if zone_2_eff == 0:
        message_z2 = messages['normal']
    elif zone_2_eff < 0:
        message_z2 = messages['proficit']
    else:
        message_z2 = messages['deficit']
    zone_1_eff_proc = round(100 - abs(zone_1_eff/240)*100 , 2)
    zone_2_eff_proc = round(100 - abs(zone_2_eff/240)*100 , 2)

    return render_template('index.html',
                           df_trucks_zone1=df_trucks_zone1,
                           df_trucks_zone2=df_trucks_zone2,
                           zone_1_eff = zone_1_eff,
                           zone_2_eff = zone_2_eff,
                           message_z1 = message_z1,
                           message_z2 = message_z2,
                           zone_1_eff_proc = zone_1_eff_proc,
                           zone_2_eff_proc = zone_2_eff_proc)

app.run(debug=True)