# tests/test_classifier.py
import pytest
from src.discovery.classifier import ColumnClassifier
from src.config import ClassificationConfig


@pytest.fixture
def classifier():
    return ColumnClassifier(ClassificationConfig())


def test_classify_primary_key(classifier):
    assert classifier.classify("id", "INT", is_primary_key=True) == "identifier"
    assert classifier.classify("user_id", "BIGINT", is_primary_key=True) == "identifier"


def test_classify_foreign_key(classifier):
    assert classifier.classify("user_id", "INT", is_foreign_key=True) == "identifier"


def test_classify_temporal(classifier):
    assert classifier.classify("created_at", "DATETIME") == "temporal"
    assert classifier.classify("event_date", "DATE") == "temporal"
    assert classifier.classify("updated_at", "TIMESTAMP") == "temporal"


def test_classify_dimension_by_type(classifier):
    assert classifier.classify("status", "VARCHAR") == "dimension"
    assert classifier.classify("mode", "ENUM") == "dimension"


def test_classify_dimension_by_name_pattern(classifier):
    assert classifier.classify("country_code", "VARCHAR") == "dimension"
    assert classifier.classify("publisher_name", "VARCHAR") == "dimension"
    assert classifier.classify("device_type", "VARCHAR") == "dimension"


def test_classify_metric_by_name_pattern(classifier):
    assert classifier.classify("click_count", "INT") == "metric"
    assert classifier.classify("total_revenue", "DECIMAL") == "metric"
    assert classifier.classify("impressions", "BIGINT") == "metric"
    assert classifier.classify("conversion_rate", "FLOAT") == "metric"


def test_classify_numeric_defaults_to_metric(classifier):
    """Numeric columns without special naming default to metric."""
    assert classifier.classify("value", "INT") == "metric"
    assert classifier.classify("score", "FLOAT") == "metric"


def test_classify_text_to_other(classifier):
    assert classifier.classify("description", "TEXT") == "other"
    assert classifier.classify("payload", "JSON") == "other"
    assert classifier.classify("notes", "LONGTEXT") == "other"


def test_classify_id_column_without_constraint(classifier):
    """Columns named *_id without PK/FK constraint are still identifiers."""
    assert classifier.classify("user_id", "INT") == "identifier"
    assert classifier.classify("order_id", "BIGINT") == "identifier"


def test_classify_respects_custom_patterns():
    """Custom patterns in config are used for classification."""
    config = ClassificationConfig(
        dimension_name_patterns=["segment", "cohort"],
        metric_name_patterns=["weight"],
    )
    c = ColumnClassifier(config)
    assert c.classify("user_segment", "VARCHAR") == "dimension"
    assert c.classify("item_weight", "FLOAT") == "metric"
