# -*- coding: utf-8 -*-
import pytest
from src.quality_monitoring.validate_data import validate_clickstream_data

def test_quality_monitoring_good_data():
    # Données conformes
    good_data = [{"user_id": "user_A", "action": "click"}]
    result = validate_clickstream_data(good_data)
    assert result["success"] is True

def test_quality_monitoring_bad_data():
    # Données corrompues (action invalide et user_id manquant)
    bad_data = [
        {"user_id": None, "action": "purchase"},
        {"user_id": "user_C", "action": "scroll"}
    ]
    result = validate_clickstream_data(bad_data)
    assert result["success"] is False