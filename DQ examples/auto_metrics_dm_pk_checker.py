import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, func
from sqlalchemy.sql import select, text
import re,os,datetime
import numpy as np,pandas as pd,copy
import time

start_time=time.time()
class EstablishConnection:
    def __init__(self, host, port, database, login, password):
        self.__host = host
        self.__port = port
        self.__login = login
        self.__password = password
        self.__database = database
        self.__engine = create_engine(
            f'vertica+vertica_python://{self.__login}:{self.__password}@{self.__host}:{self.__port}/{self.__database}')

    def connect_to_db(self):
        return self.__engine.connect()


vertica_test = EstablishConnection(host='vertica-tst.sibur.local',
                                   port='5433',
                                   database='testdb',
                                   login='MalininAYu',
                                   password='Challenge33_')

vertica_prod = EstablishConnection(host='vertica.sibur.local',
                                   port='5433',
                                   database='dwh',
                                   login='MalininAYu',
                                   password='Challenge33_')

connection_test = vertica_test.connect_to_db()
connection_prod = vertica_prod.connect_to_db()


def fetch_db_tables(tables_list,type_of_connection,schemas=False):
    dict_of_tables = {}
    if not schemas:
        schema = re.findall('(\w+)\.', tables_list[0])[0]
        meta = MetaData(schema=schema)
        for table in tables_list:
            table_name = re.findall('\.(\w+)', table)[0]
            if type_of_connection=='test':
                dict_of_tables[table] = Table(table_name, meta, autoload=True, autoload_with=connection_test)
            else:
                dict_of_tables[table] = Table(table_name, meta, autoload=True, autoload_with=connection_prod)

        return dict_of_tables

    else:
        for table in tables_list:
            schema = re.findall('(\w+)\.', table)[0]
            meta = MetaData(schema=schema)
            table_name = re.findall('\.(\w+)', table)[0]
            if type_of_connection == 'test':
                dict_of_tables[table] = Table(table_name, meta, autoload=True, autoload_with=connection_test)
            else:
                dict_of_tables[table] = Table(table_name, meta, autoload=True, autoload_with=connection_prod)

        return dict_of_tables


#dict_of_tables = fetch_db_tables(["DM_LOGISTICS_METRICS.METRICS_MAIN"])


# --------------------------------------------


def gen_sql_statement_dublicate(tables,key):
    if key=='DM_LOGISTICS_METRICS.METRICS_MAIN' or key=='DM_LOGISTICS_METRICS.V_METRICS_MAIN':
        subq_dublicates=select([tables[key].c.transportation_number,
                                tables[key].c.tech_load_ts,
                                         func.count(tables[key].c.transportation_number)]).group_by(tables[key].c.transportation_number,
                                                                                                    tables[key].c.tech_load_ts).having(func.count(tables[key].c.transportation_number)>1).alias('subq_dublicates')
        sql_statement=select([func.count(subq_dublicates.c.transportation_number)])

        return sql_statement


    elif key=='DM_LOGISTICS_METRICS.METRICS_VARIABILITY' or key=='DM_LOGISTICS_METRICS.V_METRICS_VARIABILITY':
        subq_dublicates=select([tables[key].c.year,tables[key].c.month,tables[key].c.week,
                                tables[key].c.sales_channel,tables[key].c.business_unit,
                                tables[key].c.shipment_point_code,
                                tables[key].c.delivery_point,
                                tables[key].c.tech_load_ts,
                                func.count(tables[key].c.year)]).group_by(tables[key].c.year,tables[key].c.month,tables[key].c.week,
                                                                          tables[key].c.sales_channel,
                                                                          tables[key].c.business_unit,
                                                                          tables[key].c.shipment_point_code,
                                                                          tables[key].c.delivery_point,
                                                                          tables[key].c.tech_load_ts
                                                                          ).having(func.count(tables[key].c.year)>1).alias('subq_dublicates')
        sql_statement=select([func.count(subq_dublicates.c.year)])

        return sql_statement


    elif key=='DM_LOGISTICS_METRICS.BUSINESS_UNIT_REF':
        subq_dublicates=select([tables[key].c.business_unit_code,
                                tables[key].c.shipping_point_code,
                                tables[key].c.tech_load_ts,
                                func.count(tables[key].c.business_unit_code)]).group_by(tables[key].c.business_unit_code,
                                                                                        tables[key].c.shipping_point_code,
                                                                                        tables[key].c.tech_load_ts).having(func.count(tables[key].c.business_unit_code)>1).alias('subq_dublicates')
        sql_statement=select([func.count(subq_dublicates.c.business_unit_code)])

        return sql_statement


    elif key=='DM_LOGISTICS_METRICS.CURRENCY_RATE_REF':
        subq_dublicates = select([tables[key].c.rate_date,
                                  tables[key].c.tech_load_ts,
                                  func.count(tables[key].c.rate_date)]).group_by(tables[key].c.rate_date,
                                                                                 tables[key].c.tech_load_ts).having(func.count(tables[key].c.rate_date) > 1).alias('subq_dublicates')
        sql_statement = select([func.count(subq_dublicates.c.rate_date)])

        return sql_statement


    elif key=='DM_LOGISTICS_METRICS.DELIVERY_POINT_REF':
        subq_dublicates = select([tables[key].c.delivery_point_code,
                                  tables[key].c.tech_load_ts,
                                  func.count(tables[key].c.delivery_point_code)]).group_by(tables[key].c.delivery_point_code,
                                                                                           tables[key].c.tech_load_ts).having(func.count(tables[key].c.delivery_point_code) > 1).alias('subq_dublicates')
        sql_statement = select([func.count(subq_dublicates.c.delivery_point_code)])

        return sql_statement


    elif key=='DM_LOGISTICS_METRICS.PLAN_NORM':
        subq_dublicates = select([tables[key].c.id,
                                  tables[key].c.tech_load_ts,
                                  func.count(tables[key].c.id)]).group_by(tables[key].c.id,
                                                                          tables[key].c.tech_load_ts).having(func.count(tables[key].c.id) > 1).alias('subq_dublicates')
        sql_statement = select([func.count(subq_dublicates.c.id)])

        return sql_statement


    elif key=='DM_LOGISTICS_METRICS.SHIPMENT_POINT_REF':
        subq_dublicates = select([tables[key].c.shipment_point_code,
                                  tables[key].c.tech_load_ts,
                                  func.count(tables[key].c.shipment_point_code)]).group_by(tables[key].c.shipment_point_code,
                                                                                           tables[key].c.tech_load_ts).having(func.count(tables[key].c.shipment_point_code) > 1).alias('subq_dublicates')
        sql_statement = select([func.count(subq_dublicates.c.shipment_point_code)])

        return sql_statement


def validation_rules(tables_rules,type_of_connection):
    list_of_tables=[]
    for table in tables_rules:
        for key in table.keys():
            list_of_tables.append(key)

    dict_of_tables = fetch_db_tables(list_of_tables,type_of_connection)

    dict_of_validations = {}
    for table in tables_rules:
        rules_values = {}
        for key,rules in table.items():
            for rule in rules:
                if rule=='dublicate_check':
                    pk_check = gen_sql_statement_dublicate(tables=dict_of_tables,key=key)
                    if type_of_connection=='test':
                        result = connection_test.execute(pk_check)
                    else:
                        result = connection_prod.execute(pk_check)

                    fetched_result = result.fetchall()

                    rules_values[rule] = fetched_result

        dict_of_validations[key]=rules_values

    return dict_of_validations



def dataframe_creator(data_dict,type_of_connection):
    ar=[]
    for key,value in data_dict.items():
        for rule,rule_result in value.items():
            ar.append(np.array((datetime.datetime.today().strftime('%Y-%m-%d'),type_of_connection,key,rule,rule_result[0][0])))
    dataframe=pd.DataFrame(ar,columns=['date','environment','dm_table','validation_rule','validation_result'])

    return dataframe


def dataframe_writer(dataframe):
    if not os.path.exists(r'W:\Сибур\ФЦТ\04 Направления\02 Работа с данными\DataExchange\Logistic_UCP\LogisticsMetrics\DQ_checks'):
        os.mkdir(r'W:\Сибур\ФЦТ\04 Направления\02 Работа с данными\DataExchange\Logistic_UCP\LogisticsMetrics\DQ_checks')

    if not os.path.exists(r'W:\Сибур\ФЦТ\04 Направления\02 Работа с данными\DataExchange\Logistic_UCP\LogisticsMetrics\DQ_checks\dublicate_check.csv'):
        written_dataframe=dataframe.to_csv(r'W:\Сибур\ФЦТ\04 Направления\02 Работа с данными\DataExchange\Logistic_UCP\LogisticsMetrics\DQ_checks\dublicate_check.csv',index=False,mode='a')
    else:
        written_dataframe = dataframe.to_csv(r'W:\Сибур\ФЦТ\04 Направления\02 Работа с данными\DataExchange\Logistic_UCP\LogisticsMetrics\DQ_checks\dublicate_check.csv',
                                             index=False, mode='a', header=None)

    return written_dataframe


def func_manager(tables_rules,type_of_connection):
    validation_result=validation_rules(tables_rules,type_of_connection)
    dataframe=dataframe_creator(validation_result,type_of_connection)
    print(dataframe)
    dataframe_in_folder=dataframe_writer(dataframe)
    return dataframe_in_folder









tables = [{'DM_LOGISTICS_METRICS.METRICS_MAIN': ['dublicate_check']},
          {'DM_LOGISTICS_METRICS.METRICS_VARIABILITY':['dublicate_check']},
          {'DM_LOGISTICS_METRICS.BUSINESS_UNIT_REF':['dublicate_check']},
          {'DM_LOGISTICS_METRICS.CURRENCY_RATE_REF':['dublicate_check']},
          {'DM_LOGISTICS_METRICS.DELIVERY_POINT_REF':['dublicate_check']},
          {'DM_LOGISTICS_METRICS.PLAN_NORM':['dublicate_check']},
          {'DM_LOGISTICS_METRICS.SHIPMENT_POINT_REF':['dublicate_check']},
          {'DM_LOGISTICS_METRICS.V_METRICS_MAIN': ['dublicate_check']},
          {'DM_LOGISTICS_METRICS.V_METRICS_VARIABILITY': ['dublicate_check']}]




result=func_manager(tables,'prod')
end_time=time.time()

print(end_time-start_time)

