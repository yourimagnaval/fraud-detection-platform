import pytest
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment

# Une fonction simplifiée imitans la logique de fraude Flink
def detect_fraud_logic(transaction: dict) -> dict:
    # Exemple de règle : si montant supérieur à 5000€, signaler alerte
    if transaction["amount"] > 5000:
        transaction["is_fraud"] = 1
    else:
        transaction["is_fraud"] = 0
    return transaction

def test_flink_fraud_detection():
    # Initialisation de l'environnement flink local pour le test
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Création d'un jeu de données de test (une transaction normale, une suspecte)
    mock_transactions = [
        {"transaction_id": "T1", "amount": 120.0, "is_fraud": 0},
        {"transaction_id": "T2", "amount": 7500.0, "is_fraud": 0}
    ]

    # Injection des données dans un flux flink nomade
    ds = env.from_collection(
        collection=mock_transactions,
        type_info=Types.ROW_NAMED(
            ["transaction_id", "amount", "is_fraud"],
            [Types.STRING(), Types.DOUBLE(), Types.INT()]
        )
    )

    # Application la logique de détection sur le flux Flink
    result_stream = ds.map(lambda row: detect_fraud_logic({
        "transaction_id": row.transaction_id,
        "amount": row.amount,
        "is_fraud": row.is_fraud
    }))

    # Collecte des résultats pour la vérification
    results = [data for data in result_stream.execute_and_collect()]

    # Les Assertions : Vérification que flink fonctionne
    # La transaction T1 (120€) doit rester non-frauduleuse (0)
    assert results[0]["is_fraud"] == 0
    # La transaction T2 (7500€) doit avoir été marquée comme fraude (1)
    assert results[1]["is_fraud"] == 1