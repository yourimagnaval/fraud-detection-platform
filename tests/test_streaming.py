# -*- coding: utf-8 -*-
import pytest
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

def test_flink_clickstream_fraud_detection():
    # Initialisation de l'environnement stream et table combiné pour le test
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    t_env = StreamTableEnvironment.create(env)

    # Création d'un jeu de données simulant l'activité dans la même fenêtre temporelle
    # user_A fait 11 clics (fraude), user_B fait 1 clic (normal)
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
        ("user_A", "click", "2026-06-14 10:00:08"),
        ("user_B", "click", "2026-06-14 10:00:01"),
    ]

    # Injection dans le flux DataStream de Flink
    ds = env.from_collection(
        collection=mock_clicks,
        type_info=Types.ROW_NAMED(
            ["user_id", "action", "ts_string"],
            [Types.STRING(), Types.STRING(), Types.STRING()]
        )
    )

    # Conversion en vue Table pour appliquer la requête de détection
    table = t_env.from_data_stream(ds)
    t_env.create_temporary_view("memory_src", table)

    # Requête de détection basée sur la logique de flink_job.py
    query = """
        SELECT 
            user_id,
            COUNT(action) as click_count
        FROM memory_src
        GROUP BY 
            user_id, 
            TUMBLE(TO_TIMESTAMP(ts_string), INTERVAL '10' SECOND)
        HAVING COUNT(action) > 10
    """
    
    result_table = t_env.sql_query(query)
    results = [row for row in result_table.execute().collect()]

    # Assertions : On vérifie que seul user_A est détecté et possède plus de 10 clics
    assert len(results) == 1
    assert results[0][0] == "user_A"
    assert results[0][1] == 11