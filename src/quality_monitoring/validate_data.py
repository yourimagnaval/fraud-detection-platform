# -*- coding: utf-8 -*-
import pandas as pd
import great_expectations as ge

def validate_clickstream_data(data_list):
    """
    Valide un lot de données clickstream en utilisant l'API moderne v1.x de Great Expectations.
    Prouve la gouvernance et la conformité du schéma.
    """
    # Initialisation d'un contexte de données temporaire en mémoire
    context = ge.get_context(mode="ephemeral")
    
    # Définition de la source de données Pandas
    data_source = context.data_sources.add_pandas(name="my_pandas_source")
    
    raw_df = pd.DataFrame(data_list)
    data_asset = data_source.add_dataframe_asset(name="my_clickstream_asset")
    
    # Création explicite de la définition du batch attendue par ValidationDefinition
    batch_definition = data_asset.add_batch_definition_whole_dataframe(name="my_batch_definition")
    
    # Création de la suite de règles (Expectation Suite)
    suite = context.suites.add(ge.ExpectationSuite(name="clickstream_suite"))
    
    # Ajout des règles de validation sur les colonnes
    suite.add_expectation(ge.expectations.ExpectColumnValuesToNotBeNull(column="user_id"))
    suite.add_expectation(ge.expectations.ExpectColumnValuesToBeInSet(column="action", value_set=["click"]))
    suite.add_expectation(ge.expectations.ExpectTableColumnsToMatchOrderedList(column_list=["user_id", "action"]))
    
    # Définition de la validation en passant l'objet BatchDefinition requis
    validation_definition = context.validation_definitions.add(
        ge.ValidationDefinition(
            name="my_validation_def",
            data=batch_definition,
            suite=suite
        )
    )
    
    # Exécution directe en fournissant le DataFrame réel au moment de l'évaluation
    result = validation_definition.run(batch_parameters={"dataframe": raw_df})
    
    return {
        "success": bool(result.success),
        "details": result.to_json_dict()
    }

if __name__ == "__main__":
    sample_data = [
        {"user_id": "user_A", "action": "click"},
        {"user_id": "user_B", "action": "click"}
    ]
    result = validate_clickstream_data(sample_data)
    print(f"Statut de la validation de données : {result['success']}")