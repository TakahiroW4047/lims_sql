# def query_advate_lots():
#     return """
#         SELECT
#             lot_id,
#             lot_number,        
#             material_name,
#             material_type,
#             material_datagroup,
#             date_in,
#             due_date,
#             condition
#         FROM SQA_LOTS WHERE
#             (
#                 lot_number LIKE 'TAA_____A'
#                 OR lot_number LIKE 'TAA_____'
#             )
#             AND date_in > to_date('01-JAN-17', 'DD-MON-YY')
#     """

def query_final_container_lots():
    return """
        SELECT
            lot_id,
            lot_number,
            material_name,
            material_type,
            date_in,
            due_date,
            condition
        FROM sqa_lots WHERE
            (
                (lot_number LIKE 'TAA_____A' OR lot_number LIKE 'TAA_____') OR
                (lot_number LIKE 'TVA_____A' OR lot_number LIKE 'TVA_____') OR
                (lot_number LIKE 'THA_____A' OR lot_number LIKE 'THA_____') OR
                (lot_number LIKE 'TRA_____A' OR lot_number LIKE 'TRA_____') OR
                (lot_number LIKE 'TNA_____A' OR lot_number LIKE 'TNA_____')
            )
            AND date_in > to_date('01-JAN-19', 'DD-MON-YY')
    """

def query_test_start_and_completion_time(substitution):
    return f"""
    SELECT 
        results.lot_number,
        results.material_name,
        results.material_type,
        results.lot_id,
        results.submission_id,
        results.sample_id,
        nai_workspace.worklist_id,
        results.task_id,
        results.operation,
        results.method_datagroup,
        nai_workspace.userstamp,
        results.status,
        results.condition,
        nai_workspace.timestamp AS WORKLIST_START,
        results.TEST_COMPLETED
    FROM nai_workspace RIGHT OUTER JOIN
        (
            SELECT 
                results.lot_number,
                results.material_name,
                results.material_type,
                results.lot_id,
                nai_tasks.sample_id,
                nai_tasks.submission_id,
                nai_tasks.task_id,
                nai_tasks.operation,
                method_datagroup,
                status,
                condition,
                done_date AS TEST_COMPLETED
            FROM nai_tasks INNER JOIN
                (
                    SELECT 
                        sample_id, 
                        submission_id,
                        lot_number,
                        material_name,
                        material_type,
                        lots.lot_id
                    FROM sqa_samples samples INNER JOIN
                        (
                            SELECT
                                lot_id,
                                lot_number,
                                material_name,
                                material_type
                            FROM SQA_LOTS WHERE
                                {substitution}
                        )   lots
                    ON samples.lot_id = lots.lot_id
                ) results
            ON nai_tasks.sample_id = results.sample_id
            WHERE operation IS NOT NULL
        ) results
    ON nai_workspace.task_id = results.task_id
    """

def test_query_test_start_and_completion_time():
    return f"""
    SELECT 
        results.lot_number,
        results.material_name,
        results.material_type,
        results.lot_id,
        results.submission_id,
        results.sample_id,
        nai_workspace.worklist_id,
        results.task_id,
        results.operation,
        results.method_datagroup,
        nai_workspace.userstamp,
        results.status,
        results.condition,
        nai_workspace.timestamp AS WORKLIST_START,
        results.TEST_COMPLETED
    FROM nai_workspace RIGHT OUTER JOIN
        (
            SELECT 
                results.lot_number,
                results.material_name,
                results.material_type,
                results.lot_id,
                nai_tasks.sample_id,
                nai_tasks.submission_id,
                nai_tasks.task_id,
                nai_tasks.operation,
                method_datagroup,
                status,
                condition,
                done_date AS TEST_COMPLETED
            FROM nai_tasks INNER JOIN
                (
                    SELECT 
                        sample_id, 
                        submission_id,
                        lot_number,
                        material_name,
                        material_type,
                        lots.lot_id
                    FROM sqa_samples samples INNER JOIN
                        (
                            SELECT
                                lot_id,
                                lot_number,
                                material_name,
                                material_type,
                                date_in,
                                due_date,
                                condition
                            FROM sqa_lots WHERE
                                (
                                    (lot_number LIKE 'TAA_____A' OR lot_number LIKE 'TAA_____') OR
                                    (lot_number LIKE 'TVA_____A' OR lot_number LIKE 'TVA_____') OR
                                    (lot_number LIKE 'THA_____A' OR lot_number LIKE 'THA_____') OR
                                    (lot_number LIKE 'TRA_____A' OR lot_number LIKE 'TRA_____') OR
                                    (lot_number LIKE 'TNA_____A' OR lot_number LIKE 'TNA_____')
                                )
                                AND date_in > to_date('01-JAN-19', 'DD-MON-YY')
                        )   lots
                    ON samples.lot_id = lots.lot_id
                ) results
            ON nai_tasks.sample_id = results.sample_id
            WHERE operation IS NOT NULL
        ) results
    ON nai_workspace.task_id = results.task_id
    """
def test_query_sample_receipt_and_review_dates():
    return f"""
    SELECT 
        task_list.lot_id,
        task_list.task_id,
        task_list.method_datagroup,
        history.userstamp,
        history.timestamp,
        history.final_state
    FROM naiv_instance_history history INNER JOIN
        (
            SELECT
                submission_list.lot_id,
                result.task_id,
                result.method_datagroup
            FROM nai_tasks result INNER JOIN
                (
                    SELECT
                        lots.lot_id,
                        sqa_samples.submission_id
                    FROM sqa_samples INNER JOIN
                        (
                            SELECT
                                lot_id,
                                lot_number,
                                material_name,
                                material_type,
                                date_in,
                                due_date,
                                condition
                            FROM sqa_lots WHERE
                                (
                                    (lot_number LIKE 'TAA_____A' OR lot_number LIKE 'TAA_____') OR
                                    (lot_number LIKE 'TVA_____A' OR lot_number LIKE 'TVA_____') OR
                                    (lot_number LIKE 'THA_____A' OR lot_number LIKE 'THA_____') OR
                                    (lot_number LIKE 'TRA_____A' OR lot_number LIKE 'TRA_____') OR
                                    (lot_number LIKE 'TNA_____A' OR lot_number LIKE 'TNA_____')
                                )
                                AND date_in > to_date('01-JAN-19', 'DD-MON-YY')
                        )   lots
                    ON sqa_samples.lot_id = lots.lot_id
                ) submission_list
            ON result.submission_id = submission_list.submission_id
            WHERE operation IS NOT NULL
        ) task_list
    ON history.object_id = task_list.task_id
    ORDER BY
        task_id DESC,
        timestamp DESC
    """

def query_sample_receipt_and_review_dates(substitution):
    return f"""
    SELECT 
        task_list.lot_id,
        task_list.task_id,
        task_list.method_datagroup,
        history.userstamp,
        history.timestamp,
        history.final_state
    FROM naiv_instance_history history INNER JOIN
        (
            SELECT
                submission_list.lot_id,
                result.task_id,
                result.method_datagroup
            FROM nai_tasks result INNER JOIN
                (
                    SELECT
                        lots.lot_id,
                        sqa_samples.submission_id
                    FROM sqa_samples INNER JOIN
                        (
                            SELECT lot_id FROM SQA_LOTS WHERE
                                {substitution}
                        )   lots
                    ON sqa_samples.lot_id = lots.lot_id
                ) submission_list
            ON result.submission_id = submission_list.submission_id
            WHERE operation IS NOT NULL
        ) task_list
    ON history.object_id = task_list.task_id
    ORDER BY
        task_id DESC,
        timestamp DESC
    """

def query_lot_status(substitute):
    return f"""
    SELECT 
        sqa_lots.lot_id,
        sqa_lots.lot_number,
        sqa_lots.material_name,
        sqa_lots.material_type,
        history.timestamp,
        history.class,
        history.initial_state,
        history.final_state
    FROM sqa_lots INNER JOIN
        (
            SELECT
                object_id,
                timestamp,
                class,
                initial_state,
                final_state
            FROM naiv_instance_history WHERE
                {substitute}
        ) history
        ON sqa_lots.lot_id = history.object_id
    ORDER BY
        sqa_lots.lot_number DESC
    """
