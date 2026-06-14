# -*- coding: utf-8 -*-

def validate_clickstream_data(data):
    """Valide la structure des données reçues du clickstream.
    
    Retourne True si les données sont valides, False sinon.
    """
    if not data:
        return False
        
    required_fields = ["user_id", "timestamp", "ip_address", "action"]
    
    # Vérification de la présence de tous les champs obligatoires
    for field in required_fields:
        if field not in data:
            return False
            
    return True