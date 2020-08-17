import config
import cx_Oracle
import numpy as np
import os
import pandas as pd
import psycopg2
import pytz
import time

from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Text, String, DateTime, BigInteger

from lims_query import (
    query_advate_lots,
    query_test_start_and_completion_time,
    query_sample_receipt_and_review_dates,
    query_lot_status
)

config.setup(environment='PROD')

def main():
    while True:
        result = AdvateSampleResults().result
        dispo  = AdvateDispositionHistory().result

        DbWriteSampleResult(result, table_name='sample_results')
        DbWriteDispoHistory(dispo, table_name='dispo_history')

        update_date = pd.DataFrame({"update_date": [local_datetime()]})
        DbWriteUpdateDatetime(update_date, table_name='update_date')
        print("Instance Ran At: ", datetime.now())
        time.sleep(600)
    
    return None

def local_datetime():
    dt = datetime.utcnow()
    return datetime_utc_to_local(dt)

def datetime_utc_to_local(dt):
    date = dt.replace(tzinfo=pytz.UTC)
    date = date.astimezone(pytz.timezone("US/Pacific"))
    return date


class OracleDB:
    def __init__(self):
        self.c = self.initiate_connection()

    def initiate_connection(self):
        dsn_tns = cx_Oracle.makedsn(
                os.getenv('ORACLE_DB_NAME'),
                os.getenv('ORACLE_DB_PORT'),
                service_name=os.getenv('ORACLE_DB_SERVICENAME')
            )

        conn = cx_Oracle.connect(
            user=os.getenv('ORACLE_DB_USER'),
            password=os.getenv('ORACLE_DB_PASS'),
            dsn=dsn_tns
        )
        return conn.cursor()
    
    def close_connection(self):
        self.c.close()

    def search(self, query):
        return self.query_to_dataframe(self.c, query)

    def query_to_dataframe(self, c, query):
        c.execute(query)
        description = c.description
        columns = [row[0] for row in description]
        return pd.DataFrame(c.fetchall(), columns=columns)

    def query_string_substitution(self, iterable):
        result = '('
        for item in iterable:
            result += f"{item},"
        result = result[:-1]
        result += ')'
        return result


class Advate:
    _oracle = OracleDB()
    _query = query_advate_lots()
    _df_advate_lots = _oracle.search(_query)
    lots = _df_advate_lots['LOT_ID'].values


class AdvateSampleResults(Advate):
    def __init__(self):
        oracle = OracleDB()
        self.sample_results(oracle, Advate.lots)

    def sample_results(self, oracle, advate_lots):
        query_substitute = oracle.query_string_substitution(advate_lots)
        query = query_test_start_and_completion_time(query_substitute)
        df_test_completion = oracle.search(query) 

        query = query_sample_receipt_and_review_dates(query_substitute)
        df_receipt_review_raw = oracle.search(query)

        df_receipt_review = (
            df_receipt_review_raw
            .pipe(self.remove_duplicates)
            .pipe(self.pivot_table)
        )
        
        df_merge = df_test_completion.merge(df_receipt_review, on='TASK_ID', how='outer')
        column_order = [
                'LOT_NUMBER',
                'MATERIAL_NAME',
                'LOT_ID',
                'SUBMISSION_ID',
                'SAMPLE_ID',
                'WORKLIST_ID',
                'TASK_ID',
                'OPERATION',
                'METHOD_DATAGROUP',
                'USERSTAMP',
                'STATUS',
                'CONDITION',
                'RECEIVED',
                'REJECTED',
                'WORKLIST_START',
                'TEST_COMPLETED',
                'APPROVED',
        ]
        df_merge = df_merge[column_order]
        lot_ids = df_merge['LOT_ID'].drop_duplicates().values
        self.populate_online_dates_neuchatel(df_merge, lot_ids)
        self.populate_online_dates_other(df_merge, lot_ids)
        self.calculate_stagnation_duration(df_merge)
        self.calculate_test_duration(df_merge)
        self.calculate_review_duration(df_merge)

        # print(df_merge)
        self.result = df_merge
    
    

    def remove_duplicates(self, df):
        df_approved = df[df['FINAL_STATE']=='APPROVED'].copy()
        df_online   = df[df['FINAL_STATE']=='ONLINE'].copy()
        df_rejected = df[df['FINAL_STATE']=='REJECTED'].copy()

        df_approved.sort_values(by=['TASK_ID', 'TIMESTAMP'], inplace=True, ascending=True)
        df_approved.drop_duplicates(subset='TASK_ID', inplace=True, keep='last')

        df_online.sort_values(by=['TASK_ID', 'TIMESTAMP'], inplace=True, ascending=True)
        df_online.drop_duplicates(subset='TASK_ID', inplace=True, keep='first')

        df_rejected.sort_values(by=['TASK_ID', 'TIMESTAMP'], inplace=True, ascending=True)
        df_rejected.drop_duplicates(subset='TASK_ID', inplace=True, keep='last')

        df_concat = pd.concat([df_approved, df_online, df_rejected])
        df_concat.sort_values(by=['TASK_ID', 'TIMESTAMP'], inplace=True, ascending=True)

        return df_concat

    # Pivot table to show test status in columns
    def pivot_table(self, df):
        df_result = df.pivot(index='TASK_ID', columns='FINAL_STATE', values='TIMESTAMP')
        df_result.rename(columns={'ONLINE':'RECEIVED'}, inplace=True)

        return df_result 

    def populate_online_dates_neuchatel(self, df, lot_ids):
        is_neuchatel = df['METHOD_DATAGROUP']=='NEUCHATEL'
        is_received = df['RECEIVED'].notnull()
        is_not_received = df['RECEIVED'].isnull()
                
        for lot in lot_ids:
            try:
                is_lot = df['LOT_ID']==lot
                
                # Find sample received date in Neuchatel per lot
                boolean_neuchatel = (
                    is_lot & 
                    is_neuchatel & 
                    is_received)
                
                sample_received_date_in_neuchatel = df.loc[boolean_neuchatel, 'RECEIVED'].values.min()

                # Assign 'RECEIVED' to Neuchatel Lot
                boolean_neuchatel_lot = (
                    is_lot & 
                    is_neuchatel & 
                    is_not_received)
                
                df.loc[boolean_neuchatel_lot, 'RECEIVED'] = sample_received_date_in_neuchatel

                # Assign 'WORKLIST_START' to all Neuchatel Lot
                boolean_neuchatel_lot = (
                    is_lot & 
                    is_neuchatel)
                
                df.loc[boolean_neuchatel_lot, 'WORKLIST_START'] = sample_received_date_in_neuchatel
            except:
                # print("Neuchatel Lot Skipped: " + str(lot))
                pass
                    
        return None

    def populate_online_dates_other(self, df, lot_ids):
        is_not_neuchatel = df['METHOD_DATAGROUP']!='NEUCHATEL'
        is_received = df['RECEIVED'].notnull()
        is_not_received = df['RECEIVED'].isnull()
        has_no_worklist = df['WORKLIST_START'].isnull()
        
        for lot in lot_ids:
            try:
                is_lot = df['LOT_ID']==lot
            
                # Find sample received date in  per lot
                boolean_other = (
                    is_lot & 
                    is_not_neuchatel & 
                    is_received)
                
                sample_received_date_in_other = df.loc[boolean_other, 'RECEIVED'].values.min()
                
                # Assign 'RECEIVED' to Other Lot
                boolean_other_lot = (
                    is_lot & 
                    is_not_neuchatel & 
                    is_not_received)
                
                df.loc[boolean_other_lot, 'RECEIVED'] = sample_received_date_in_other
                
                # Assign 'WORKLIST_START' to all non Neuchatel Lot
                boolean_other_lot = (
                    is_lot & 
                    is_not_neuchatel & 
                    has_no_worklist)
                
                df.loc[boolean_other_lot, 'WORKLIST_START'] = sample_received_date_in_other   
            except:
                # print("Other Lot Skipped: " + str(lot)) 
                pass
        return None

    def calculate_stagnation_duration(self, df):
        # If "LOGGED/ONLINE", then calculate duration from "Received Date" to "Today"
        boolean_logged_online = df['STATUS'].isin(['LOGGED']) & df['CONDITION'].isin(['ONLINE'])
        
        # If test Condition is NOT "INCOMPLETE" & it has a worklist, then calculate duration from "Received" to "Worklist"
        boolean_test_incomplete_with_worklist = (~df['CONDITION'].isin(['INCOMPLETE'])) & df['WORKLIST_START'].notnull()
        
        # Apply calculation with boolean mask
        df.loc[boolean_logged_online, 'STAGNATION_DURATION'] = np.datetime64('now') - df['RECEIVED']
        df.loc[boolean_test_incomplete_with_worklist, 'STAGNATION_DURATION'] = df['WORKLIST_START'] - df['RECEIVED']

        # Change type to hours
        df.loc[:,'STAGNATION_DURATION'].astype('timedelta64[h]')
        
    def calculate_test_duration(self, df):
        # If "ACTIVE/ONLINE" and has Worklist, then calculate from "Worklist Start" to "Today"
        boolean_activeonline_has_worklist = (
            (df['STATUS'].isin(['ACTIVE'])) &
            (df['CONDITION']).isin(['ONLINE']) &
            (df['WORKLIST_START'].notnull())
        )
        
        # If "ACTIVE/ONLINE" and has no Worklist, then calculate from "Received" to "Today"
        boolean_activeonline_no_worklist = (
            (df['STATUS'].isin(['ACTIVE'])) &
            (df['CONDITION'].isin(['ONLINE'])) &
            (df['WORKLIST_START'].isnull())
        )
        
        # If "TEST_COMPLETED" and with Worklist, calculate from "Worklist_Start" to "Completion"
        boolean_testcompleted_with_worklist = (
            (df['TEST_COMPLETED'].notnull()) &
            (df['WORKLIST_START'].notnull())
        )
        
        # If "TEST_COMPLETED" and has no Worklist, calculate from "Received Date" to "Completion"
        boolean_testcompleted_no_worklist = (
            (df['TEST_COMPLETED'].notnull()) &
            (df['WORKLIST_START'].isnull())
        )
        
        # Apply calculations with boolean mask
        df.loc[boolean_activeonline_has_worklist,   'TEST_DURATION'] = np.datetime64('now') - df['WORKLIST_START']
        df.loc[boolean_activeonline_no_worklist,    'TEST_DURATION'] = np.datetime64('now') - df['RECEIVED']
        df.loc[boolean_testcompleted_with_worklist, 'TEST_DURATION'] = df['TEST_COMPLETED'] - df['WORKLIST_START']
        df.loc[boolean_testcompleted_no_worklist,   'TEST_DURATION'] = df['TEST_COMPLETED'] - df['RECEIVED']
        
        # Change type to hours
        df.loc[:,'TEST_DURATION'].astype('timedelta64[h]')

    def calculate_review_duration(self, df):
        # If "TEST_COMPLETED" and Condition = "ONLINE", then calculate from "Test Completion" to "Today"
        boolean_testcompleted_condition_online = (
            (df['TEST_COMPLETED'].notnull()) &
            (df['CONDITION'].isin(['ONLINE']))
        )
        
        # If "TEST_COMPLETED" and Condition = "APPROVED", then calculate from "Test Completion" to "Approved"
        boolean_testcompleted_condition_approved = (
            (df['TEST_COMPLETED'].notnull()) &
            (df['CONDITION'].isin(['APPROVED']))
        )
        
        # If "TEST_COMPLETED" and Condition = "Rejected", then calculate from "Received" to "Rejected"
        boolean_testcompleted_condition_rejected = (
            (df['TEST_COMPLETED'].notnull()) &
            (df['CONDITION'].isin(['REJECTED']))
        )
        
        # Apply calculations with boolean mask
        df.loc[boolean_testcompleted_condition_online,   'REVIEW_DURATION'] = np.datetime64('now') - df['TEST_COMPLETED']
        df.loc[boolean_testcompleted_condition_approved, 'REVIEW_DURATION'] = df['APPROVED'] - df['TEST_COMPLETED']
        df.loc[boolean_testcompleted_condition_rejected, 'REVIEW_DURATION'] = df['REJECTED'] - df['RECEIVED']
        
        # Change type to hours
        df.loc[:,'REVIEW_DURATION'].astype('timedelta64[h]')


class AdvateDispositionHistory(Advate):
    def __init__(self):
        oracle = OracleDB()
        self._disposition_history(oracle, Advate.lots)

    def _disposition_history(self, oracle, advate_lots):
        string_substitution = oracle.query_string_substitution(advate_lots)
        query = query_lot_status(string_substitution)
        df_dispo_history = oracle.search(query)
        self.result = self._datawrangling(df_dispo_history)

    def _datawrangling(self, df):
        df_1 = (df
            .pipe(self._remove_canceled_lots)
            .pipe(self._remove_duplicates)
            )

        df_2 = pd.pivot(df_1, index='LOT_ID', columns='FINAL_STATE', values='TIMESTAMP').reset_index()
        df_3 = (df_1
            .pipe(self._column_pruning)
            .pipe(self._truncate_lot_ids)
        )
        self._column_pruning(df_1)

        df_4 = df_2.merge(df_3, on='LOT_ID', how='left')

        df_5 = self._reorder_columns(df_4)

        return df_5
    
    def _remove_canceled_lots(self, df):
        lots_to_remove = []
        lots = df['LOT_ID'].unique()
        for lot in lots:
            final_state = df[df['LOT_ID']==lot].sort_values(by='TIMESTAMP', ascending=False)['FINAL_STATE'].values[0]
            if final_state == 'CANCELED':
                lots_to_remove.append(lot)
        return df[~df['LOT_ID'].isin(lots_to_remove)]

    def _remove_duplicates(self, df):
        def drop_option_ready_for_release(df):
            result = df.sort_values(by=['LOT_ID', 'TIMESTAMP'], ascending=True)
            result.drop_duplicates(subset='LOT_ID', keep='last', inplace=True)
            return result
            
        def drop_option_complete(df):
            result = df.sort_values(by=['LOT_ID', 'TIMESTAMP'], ascending=True)
            result.drop_duplicates(subset='LOT_ID', keep='last', inplace=True)
            return result
            
        def drop_option_active(df):
            result = df.sort_values(by=['LOT_ID', 'TIMESTAMP'], ascending=True)
            result.drop_duplicates(subset='LOT_ID', keep='first', inplace=True)
            return result
            
        def drop_option_dispositioned(df):
            result = df.sort_values(by=['LOT_ID', 'TIMESTAMP'], ascending=True)
            result.drop_duplicates(subset='LOT_ID', keep='last', inplace=True)
            return result
        
        def drop_option_online(df):
            result = df.sort_values(by=['LOT_ID', 'TIMESTAMP'], ascending=True)
            result.drop_duplicates(subset='LOT_ID', keep='first', inplace=True)
            return result
            
        def drop_option_suspect(df):
            result = df.sort_values(by=['LOT_ID', 'TIMESTAMP'], ascending=True)
            result.drop_duplicates(subset='LOT_ID', keep='first', inplace=True)
            return result
        
        function_mapping = {'READY FOR RELEASE':drop_option_ready_for_release,
                            'COMPLETE':drop_option_complete,
                            'ACTIVE': drop_option_active,
                            'DISPOSITIONED': drop_option_dispositioned,
                            'ONLINE': drop_option_online,
                            'SUSPECT': drop_option_suspect}
        dfs = {}
        final_df = pd.DataFrame()
        
        for selection in function_mapping.keys():
            df_temp = df[df['FINAL_STATE']==selection]
            dfs[selection] = df_temp
            
        for keys, function in function_mapping.items():
            df = dfs[keys]
            result = function(df)
            final_df = pd.concat([final_df, result], ignore_index=True)
            
        final_df.sort_values(by=['LOT_NUMBER', 'MATERIAL_NAME'], inplace=True, ascending=False)
        
        return final_df

    def _column_pruning(self, df):
        return df[['LOT_ID', 'LOT_NUMBER', 'MATERIAL_NAME']]

    def _reorder_columns(self, df):
        return df[['LOT_NUMBER', 'LOT_ID', 'MATERIAL_NAME', 'ACTIVE', 'COMPLETE', 'READY FOR RELEASE', 'DISPOSITIONED', 'SUSPECT']]

    def _truncate_lot_ids(self, df):
        return df.drop_duplicates(subset='LOT_ID')


class PostgresDB:
    _db_uri = os.getenv('SQLALCHEMY_DB_URI')
    engine = create_engine(_db_uri, echo=False)

    def read(self, table_name):
        return pd.read_sql_table(table_name, con=engine)


class DbWriteSampleResult(PostgresDB):
    def __init__(self, df, table_name):
        self.table_name=table_name
        self._db_write(df)   

    def _db_write(self, df):
        df.to_sql(
            self.table_name,
            PostgresDB.engine,
            if_exists='replace',
            index=False,
            chunksize=500,
            dtype={
                "LOT_NUMBER": Text,
                "MATERIAL_NAME": Text,
                "LOT_ID": Integer,
                "SUBMISSION_ID": Integer,
                "SAMPLE_ID": Integer,
                "WORKLIST_ID": Integer,
                "TASK_ID": Integer,
                "OPERATION": Text,
                "RECEIVED": DateTime,
                "REJECTED": DateTime,
                "WORKLIST_START": DateTime,
                "TEST_COMPLETED": DateTime,
                "APPROVED": DateTime,
                "STAGNATION_DURATION": BigInteger,
                "TEST_DURATION":  BigInteger,
                "REVIEW_DURATION":  BigInteger
            }
        )


class DbWriteDispoHistory(PostgresDB):
    def __init__(self, df, table_name):
        self.table_name = table_name
        self._db_write(df)

    def _db_write(self, df):
        df.to_sql(
            self.table_name,
            PostgresDB.engine,
            if_exists='replace',
            index=False,
            chunksize=500,
            dtype={
                "LOT_NUMBER": Text,
                "LOT_ID": Integer,
                "MATERIAL_NAME": Text,
                "ACTIVE": DateTime,
                "COMPLETE": DateTime,
                "READY FOR RELEASE": DateTime,
                "DISPOSITIONED": DateTime,
                "SUSPECT": DateTime
            }
        )


class DbWriteUpdateDatetime(PostgresDB):
    def __init__(self, df, table_name):
        self.table_name = table_name
        self._db_write(df)

    def _db_write(self, df):
        df.to_sql(
            self.table_name,
            PostgresDB.engine,
            if_exists='replace',
            index=False,
            chunksize=500,
            dtype={
                "UPDATE_DATETIME": DateTime
            }
        )


if __name__ == "__main__":
    main()
    