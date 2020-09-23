def query_final_container_lots(cutoff_date):
    return f"""
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
            (
                (lot_number LIKE 'TAA_____A' OR lot_number LIKE 'TAA_____') OR
                (lot_number LIKE 'TVA_____A' OR lot_number LIKE 'TVA_____') OR
                (lot_number LIKE 'THA_____A' OR lot_number LIKE 'THA_____') OR
                (lot_number LIKE 'TRA_____A' OR lot_number LIKE 'TRA_____') OR
                (lot_number LIKE 'TNA_____A' OR lot_number LIKE 'TNA_____')
            )
            OR 
            (
                (material_name='RAHF BDS' AND material_type IN ('CELL_CULTURE', 'CELL_CULTURE1', 'PURIFICATION')) 
                OR (material_name='RAHF_PFM_BDS')
                OR (material_name='BAX 855' AND material_type='BULK MANUFACTURING')
            )
        ) AND date_in > to_date('{cutoff_date}', 'DD-MON-YY')
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
            WHERE 
                operation IS NOT NULL
        ) results
    ON nai_workspace.task_id = results.task_id
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

def query_operation_sop(cutoff_date):
    return f"""
    SELECT
        task_id,
        operation,
        b.text_value
    FROM
        nai_tasks,
        table(nai_tasks.attributes) b
    WHERE
        nai_tasks.timestamp > to_date('{cutoff_date}', 'DD-MON-YY')
        AND b.name = 'SOP NUMBER'
        AND operation IS NOT NULL
    ORDER BY operation
    """

def query_update_dispo_received_date(table_name, data):
    return f"""
        UPDATE {table_name} SET
            "ACTIVE" = data.new_received_date
        FROM (VALUES
            {data}
        ) AS data(lot_number, new_received_date)
        WHERE "LOT_NUMBER" = data.lot_number
    """