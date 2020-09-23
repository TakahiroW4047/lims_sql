import config
import cx_Oracle
import logging
import numpy as np
import os
import pandas as pd
import psycopg2
import pytz
import re
import time
import threading

from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Text, String, DateTime, BigInteger

from lims_query import (
    query_final_container_lots,
    query_test_start_and_completion_time,
    query_sample_receipt_and_review_dates,
    query_lot_status,
    query_operation_sop,
    query_update_dispo_received_date
)

# config.setup(environment='DEV')

def main():
    def task_3_month_results(): # Run on every hour
        cutoff_month=3
        tablename_dispo_history = 'dispo_history'
        tablename_sample_results = 'sample_results'
        tablename_update_date = 'update_date'
        func_list = [
            lambda x=cutoff_month, y=tablename_dispo_history: update_disposition_history(
                cutoff_month_count=x, table_name=y),
            lambda x=cutoff_month, y=tablename_dispo_history, z=tablename_sample_results : update_sample_results(
                cutoff_month_count=x, dispo_table_name=y, result_table_name=z),
            lambda x=tablename_dispo_history, y=tablename_sample_results: update_disposition_history_received(
                dispo_table_name=x, result_table_name=y),
            lambda x=tablename_update_date: update_date(x)
        ]

        has_ran = False
        while True:
            time.sleep(1)
            start_time = datetime.now()
            trigger = 55
            if local_datetime().minute == trigger and has_ran==False:
                logging.info(local_datetime_string() + '- Task Initiated, 3 month results')
                for func in func_list:
                    func()
                has_ran=True
                logging.info(local_datetime_string() + '- Task Completed, 3 month results, duration=' + str(datetime.now()-start_time))
            if local_datetime().minute != trigger:
                has_ran=False

    def task_3_year_results():  # Run once a day at midnight
        cutoff_month=36
        tablename_dispo_history = 'dispo_history_3_years'
        tablename_sample_results = 'sample_results_3_years'
        tablename_update_date = 'update_date_3_years'
        func_list = [
            lambda x=cutoff_month, y=tablename_dispo_history: update_disposition_history(
                cutoff_month_count=x, table_name=y),
            lambda x=cutoff_month, y=tablename_dispo_history, z=tablename_sample_results: update_sample_results(
                cutoff_month_count=x, dispo_table_name=y, result_table_name=z),
            lambda x=tablename_dispo_history, y=tablename_sample_results: update_disposition_history_received(
                dispo_table_name=x, result_table_name=y),
            lambda x=tablename_update_date: update_date(x)
        ]

        has_ran = False
        internal_day = local_datetime().day
        while True:
            time.sleep(10)
            today = local_datetime().day
            start_time = datetime.now()
            if internal_day == today and has_ran==False:
                logging.info(local_datetime_string() + '- Task Initiated, 3 year results')
                for func in func_list:
                    func()
                has_ran=True
                logging.info(local_datetime_string() + '- Task Completed, 3 year results, duration=' + str(datetime.now()-start_time))
            if internal_day != today:
                has_ran=False
                internal_day = today

    def update_disposition_history(cutoff_month_count, table_name):
        dispo  = DispositionHistory(cutoff_month_count).result    # Run time 7min 55sec
        DbWriteDispoHistory(dispo, table_name=table_name)

    def update_sample_results(cutoff_month_count, dispo_table_name, result_table_name):
        result = SampleResults(cutoff_month_count=cutoff_month_count, table_name=dispo_table_name).result # Run time 4min
        DbWriteSampleResult(result, table_name=result_table_name)

    def update_disposition_history_received(dispo_table_name, result_table_name):
        df_accurate_received_dates = DispositionHistoryReceivedDatesFixed(result_table_name).result
        print(df_accurate_received_dates[df_accurate_received_dates['LOT_NUMBER']=='A20C24UX01'])
        DbUpdateDispoHistory(df_accurate_received_dates, table_name=dispo_table_name)

    def update_date(table_name):
        update_date = pd.DataFrame({"update_date": [local_datetime_string()]})
        DbWriteUpdateDatetime(update_date, table_name=table_name)
    
    logging.basicConfig(filename='log/lims_sql.log', format='%(levelname)s: %(message)s', level=logging.DEBUG)
    logging.info(local_datetime_string() + '- App Initiated')

    threads = list()
    func_list = [task_3_month_results, task_3_year_results]
    # func_list = [task_3_year_results]
    # func_list = [lambda: print('hello')]

    for func in func_list:
        thread = threading.Thread(target=func)
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    return None

def local_datetime():
    dt = datetime.utcnow()
    return datetime_utc_to_local(dt)

def local_datetime_string():
    dt = datetime_utc_to_local(datetime.utcnow())
    dt_string = str(
        datetime.strftime(dt, "%d%b%y %H:%M")
        ).upper()
    return dt_string

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

    def query_string_substitution(self, column_name, iterable):
        iterable = iterable
        result = '('
        while any(iterable):
            chunk = iterable[:900]
            rest = iterable[900:]
            iterable = rest
            temp_numbers = '('
            for number in chunk:
                temp_numbers += (str(number)+',')
            temp_numbers = temp_numbers[:-1] + ')'
            result += (column_name + ' IN ' + temp_numbers + ' OR ')
        result = result[:-3] + ')' 
        return result


class LotNumberFinalContainer():
    def __init__(self, cutoff_month_count):
        _past = datetime.now() - timedelta(days=cutoff_month_count*31)
        _cutoff_date = datetime.strftime(_past, "%d-%b-%y").upper()

        _oracle = OracleDB()
        _query = query_final_container_lots(_cutoff_date)
        _df_lots = _oracle.search(_query)

        df_advate = self._filter_advate(_df_lots)
        df_vonvendi = self._filter_vonvendi(_df_lots)
        df_hemofil = self._filter_hemofil(_df_lots)
        df_recombinant = self._filter_recombinant(_df_lots)
        df_rixubis = self._filter_rixubis(_df_lots)
        df_bds = self._filter_bds(_df_lots)

        self.alllots = self._return_lot_values_from(pd.concat([df_advate, df_vonvendi, df_hemofil, df_recombinant, df_rixubis, df_bds], ignore_index=True))
        self.advate = self._return_lot_values_from(df_advate)
        self.vonvendi = self._return_lot_values_from(df_vonvendi)
        self.hemofil = self._return_lot_values_from(df_hemofil)
        self.recombinant = self._return_lot_values_from(df_recombinant)
        self.rixubis = self._return_lot_values_from(df_rixubis)
        self.bds = self._return_lot_values_from(df_bds)


    def _filter_advate(self, df):
        df = df[df['LOT_NUMBER'].str.startswith('TAA')]
        return df

    def _filter_vonvendi(self, df):
        boolean = (
            df['LOT_NUMBER'].str.startswith('TVA') &
            df['MATERIAL_NAME'].isin(['RVWF']) & 
            (
                df['MATERIAL_TYPE'].isin(['BULK DRUG'])) | (df['MATERIAL_TYPE'].isin(['FINAL CONTAINER'])
            )
        )
        return df[boolean]

    def _filter_hemofil(self, df):
        boolean = (
            df['LOT_NUMBER'].str.startswith('THA') &
            (
                (df['MATERIAL_NAME'].isin(['AHF-M BULK']) & df['MATERIAL_TYPE'].isin(['FORM_FINISH'])) |
                (df['MATERIAL_NAME'].isin(['AHFM FINAL CONTAINER']) & df['MATERIAL_TYPE'].isin(['FINAL_CONTAINER']))
            )
        )
        return df[boolean]

    def _filter_recombinant(self, df):
        boolean = (
            df['LOT_NUMBER'].str.startswith('TRA') &
            (
                (df['MATERIAL_NAME'].isin(['RAHF BDS']) & df['MATERIAL_TYPE'].isin(['FORM_FINISH'])) |
                (df['MATERIAL_NAME'].isin(['RAHF FINAL CONTAINER']) & df['MATERIAL_TYPE'].isin(['FINAL_CONTAINER']))
            )
        )
        return df[boolean]

    def _filter_rixubis(self, df):
        boolean = (
            df['LOT_NUMBER'].str.startswith('TNA') &
            (
                (df['MATERIAL_NAME'].isin(['RFIX_BDS']) & df['MATERIAL_TYPE'].isin(['FORM_FINISH'])) |
                (df['MATERIAL_NAME'].isin(['RFIX_FINAL_CONTAINER']) & df['MATERIAL_TYPE'].isin(['FINAL_CONTAINER']))
            )
        )
        return df[boolean]

    def _filter_bds(self, df):
        boolean = (
            (df['MATERIAL_NAME'].isin(['RAHF BDS']) & df['MATERIAL_TYPE'].isin(['CELL_CULTURE'])) |
            (df['MATERIAL_NAME'].isin(['RAHF_PFM_BDS', 'BAX 855']))
        )
        return df[boolean]

    def _return_lot_values_from(self, df):
        return df['LOT_ID'].values


class SampleResults():
    def __init__(self, cutoff_month_count, table_name):
        oracle = OracleDB()
        postgres = PostgresDB()

        lots = postgres.read(table_name=table_name)['LOT_ID'].values
        self.result = self.sample_results(oracle, lots)

        df_taskid_operation_sops = OperationSOPCombinations(cutoff_month_count).result
        df_operation_sops_supplement = pd.read_csv('operation_to_sop.csv')

        self.result = self.merge_operation_sops(
            df_taskid_operation_sops, 
            df_operation_sops_supplement)

        self.extract_sop_from_operation()
        self.add_group_based_on_sop()

    def sample_results(self, oracle, lots):
        query_substitute = oracle.query_string_substitution('lot_id', lots)
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
                'MATERIAL_TYPE',
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

        return df_merge
    
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

    def merge_operation_sops(self, df_taskid_operation_sops, df_operation_sops_supplement):
        df = self.result.merge(df_taskid_operation_sops[['TASK_ID', 'SOP']], how='left', on='TASK_ID')

        def supplement_sop(operation):
            try:
                supplemental_sop = df_operation_sops_supplement[df_operation_sops_supplement['OPERATION']==operation]['SOP'].values[0]
            except:
                supplemental_sop = None
            return supplemental_sop

        boolean = df['SOP'].isnull()
        df.loc[boolean, 'SOP'] = df['OPERATION'].apply(supplement_sop)

        column_order = [
                'LOT_NUMBER',
                'MATERIAL_NAME',
                'MATERIAL_TYPE',
                'LOT_ID',
                'SUBMISSION_ID',
                'SAMPLE_ID',
                'WORKLIST_ID',
                'TASK_ID',
                'OPERATION',
                'SOP',
                'METHOD_DATAGROUP',
                'USERSTAMP',
                'STATUS',
                'CONDITION',
                'RECEIVED',
                'REJECTED',
                'WORKLIST_START',
                'TEST_COMPLETED',
                'APPROVED',
                'STAGNATION_DURATION', 
                'TEST_DURATION', 
                'REVIEW_DURATION'
        ]
        return df[column_order]

    def extract_sop_from_operation(self):
        def regex_operation_for_sop(operation_string):
            regex_list = [
                '([A-Z]{2})-([0-9]{2})-(.{2}[0-9]{3})',
                '([A-Z]{2}.{2}[0-9]{5})',
                '([A-Z]{2}[0-9]{3})'
            ]
            for regex in regex_list:
                re_object = re.search(regex, operation_string)
                if re_object != None:
                    result =  "".join(re_object.groups())
                    if len(result)==5:
                        result = 'TO11' + result
                    return result
            return None

        boolean = self.result['SOP'].isnull()
        self.result.loc[boolean, 'SOP'] = self.result['OPERATION'].apply(regex_operation_for_sop)

    def add_group_based_on_sop(self):
        def find_group(sop):
            try:
                group = group_sop[group_sop['SOP']==sop]['Group'].values[0]
            except:
                group = None
            return group

        group_sop = pd.read_csv('group_split_by_sop.csv')
        self.result['GROUP'] = self.result['SOP'].apply(find_group)


class DispositionHistory():
    def __init__(self, cutoff_month_count):
        oracle = OracleDB()
        samplelots = LotNumberFinalContainer(cutoff_month_count)
        self._disposition_history(oracle, samplelots.alllots)

    def _disposition_history(self, oracle, lots):
        string_substitution = oracle.query_string_substitution('object_id', lots)
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
        return df[['LOT_ID', 'LOT_NUMBER', 'MATERIAL_NAME', 'MATERIAL_TYPE']]

    def _reorder_columns(self, df):
        return df[['LOT_NUMBER', 'LOT_ID', 'MATERIAL_NAME', 'MATERIAL_TYPE', 'ACTIVE', 'COMPLETE', 'READY FOR RELEASE', 'DISPOSITIONED', 'SUSPECT']]

    def _truncate_lot_ids(self, df):
        return df.drop_duplicates(subset='LOT_ID')


class OperationSOPCombinations():
    def __init__(self, cutoff_month_count):
        oracle = OracleDB()
        _cutoff_date = self.month_to_date_conversion(cutoff_month_count)
        _query = query_operation_sop(cutoff_date=_cutoff_date)
        self.result = oracle.search(_query)
        self.result.columns = ['TASK_ID', 'OPERATION', 'SOP']
    
    def month_to_date_conversion(self, cutoff_month_count):
        _past = datetime.now() - timedelta(days=cutoff_month_count*31)
        return datetime.strftime(_past, "%d-%b-%y").upper() 

        
class DispositionHistoryReceivedDatesFixed():
    def __init__(self, table_name):
        postgres = PostgresDB()
        df = postgres.read(table_name=table_name)
        df_accurate_received_dates = self.find_accurate_received_dates(df)
        self.result = df_accurate_received_dates

    def find_accurate_received_dates(self, df):
        df_temp = df[df['METHOD_DATAGROUP'] != 'MFG_DATA_ENTRY']
        return df_temp.groupby(['LOT_NUMBER']).agg({'RECEIVED':np.min}).reset_index()


class PostgresDB:
    def __init__(self):
        _db_uri = os.getenv('SQLALCHEMY_DB_URI')
        self.engine = create_engine(_db_uri, echo=False)

    def read(self, table_name):
        return pd.read_sql_table(table_name, con=self.engine)


class DbWriteSampleResult():
    def __init__(self, df, table_name):
        self.postgres = PostgresDB()
        self.table_name=table_name
        self._db_write(df)   

    def _db_write(self, df):
        df.to_sql(
            self.table_name,
            self.postgres.engine,
            if_exists='replace',
            index=False,
            chunksize=500,
            dtype={
                "LOT_NUMBER": Text,
                "MATERIAL_NAME": Text,
                "MATERIAL_TYPE": Text,
                "LOT_ID": Integer,
                "SUBMISSION_ID": Integer,
                "SAMPLE_ID": Integer,
                "WORKLIST_ID": Integer,
                "TASK_ID": Integer,
                "OPERATION": Text,
                "SOP": Text,
                "GROUP": Text,
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


class DbWriteDispoHistory():
    def __init__(self, df, table_name):
        self.postgres = PostgresDB()
        self.table_name = table_name
        self._db_write(df)

    def _db_write(self, df):
        df.to_sql(
            self.table_name,
            self.postgres.engine,
            if_exists='replace',
            index=False,
            chunksize=500,
            dtype={
                "LOT_NUMBER": Text,
                "LOT_ID": Integer,
                "MATERIAL_NAME": Text,
                "MATERIAL_TYPE": Text,
                "ACTIVE": DateTime,
                "COMPLETE": DateTime,
                "READY FOR RELEASE": DateTime,
                "DISPOSITIONED": DateTime,
                "SUSPECT": DateTime
            }
        )


class DbUpdateDispoHistory():
    def __init__(self, df, table_name):
        self.postgres = PostgresDB()
        self.table_name = table_name
        self.data = ""
        self.create_update_sql_query_string(df)
        self.update_query()


    def create_update_sql_query_string(self, df):
        def pull_row_data(row):
            lot = row['LOT_NUMBER']
            date = row['RECEIVED']
            data_string = f"(\'{lot}\', \'{date}\'::timestamp),"
            self.data += data_string
            if lot == 'TAA20013A': print('Its here!: ', date)
            return None

        df.apply(pull_row_data, axis=1)
        self.data = self.data[:-1]
        return None

    def update_query(self):
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_DB_HOST'),
            database=os.getenv('POSTGRES_DB_NAME'),
            user=os.getenv('POSTGRES_DB_USER'),
            password=os.getenv('POSTGRES_DB_PASS')
        )
        cur = conn.cursor()
        
        query = query_update_dispo_received_date(self.table_name, self.data)
        print(query[:200])
        cur.execute(query)
        conn.commit()
        print('Done!')
        cur.close()
        conn.close()
        


class DbWriteUpdateDatetime():
    def __init__(self, df, table_name):
        self.postgres = PostgresDB()
        self.table_name = table_name
        self._db_write(df)

    def _db_write(self, df):
        df.to_sql(
            self.table_name,
            self.postgres.engine,
            if_exists='replace',
            index=False,
            chunksize=500,
            dtype={
                "UPDATE_DATETIME": Text
            }
        )


if __name__ == "__main__":
    main()