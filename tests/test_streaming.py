# -*- coding: utf-8 -*-
import pytest
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, Schema

def test_flink_clickstream_fraud_detection():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    t_env = StreamTableEnvironment.create(env)

    # Ajout d'un timestamp fictif pour contrôler manuellement la progression du temps
    mock_clicks = [
        ("user_A", "click", "2026-06-14 10:00:01"),
        ("user_A", "click", "2026-06-14 10:00:02"),
        ("user_A", "click", "2026-06-14 10:00:02"),
        ("user_A", "click", "2026-06-14 10:00:03"),
        ("user_A", "click", "2026-06-14 10:00:04"),
        ("user_A", "click", "2026-06-14 10:00:05"),
        ("user_A", "click", "2026-06-14 10:00:05"),
        ("user_A", "click", "2026-06-14 10:00:06"),
        ("user_A", "click", "2026-06-14 10:00:07"),
        ("user_A", "click", "2026-06-14 10:00:07"),
        ("user_A", "click", "2026-06-14 10:00:08"), # 11 clics
        ("user_B", "click", "2026-06-14 10:00:01"),
        # Cet événement sert à faire avancer le Watermark au-delà des 10 secondes pour fermer la fenêtre
        ("user_B", "click", "2026-06-14 10:00:12"), 
    ]

    ds = env.from_collection(
        collection=mock_clicks,
        type_info=Types.ROW_NAMED(
            ["user_id", "action", "ts_str"],
            [Types.STRING(), Types.STRING(), Types.STRING()]
        )
    )

    # Déclaration du schéma avec conversion en TIMESTAMP et application du Watermark
    schema = Schema.new_builder() \
        .column("user_id", "STRING") \
        .column("action", "STRING") \
        .column_by_expression("row_time", "TO_TIMESTAMP(ts_str)") \
        .watermark("row_time", "row_time - INTERVAL '1' SECOND") \
        .build()

    table = t_env.from_data_stream(ds, schema)
    t_env.create_temporary_view("memory_src", table)

    # Exécution de la requête sur le row_time (Event Time)
    query = """
        SELECT 
            user_id,
            COUNT(action) as click_count
        FROM memory_src
        GROUP BY 
            user_id, 
            TUMBLE(row_time, INTERVAL '10' SECOND)
        HAVING COUNT(action) > 10
    """
    
    result_table = t_env.sql_query(query)
    results = [row for row in result_table.execute().collect()]

    assert len(results) == 1
    assert results[0][0] == "user_A"
    assert results[0][1] == 11