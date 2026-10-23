import pytest

from wstlr.embedable import EmbedableTable


@pytest.fixture
def observations_csv(tmp_path):
    path = tmp_path / "observations.csv"
    path.write_text("Subject Id,Value\nS1,10\nS1,20\nS2,30\n")
    return path


def test_load_data_normalizes_column_names(observations_csv):
    table = EmbedableTable("observations", "subject", "Subject Id")
    table.load_data(observations_csv)
    assert table.column_names == ["subject_id", "value"]


def test_get_rows_returns_all_matching_rows_with_table_name_tag(observations_csv):
    table = EmbedableTable("observations", "subject", "Subject Id")
    table.load_data(observations_csv)

    rows = table.get_rows("S1")

    assert rows == [
        {"table_name": "observations", "subject_id": "S1", "value": "10"},
        {"table_name": "observations", "subject_id": "S1", "value": "20"},
    ]


def test_get_rows_returns_empty_list_for_unknown_id(observations_csv):
    table = EmbedableTable("observations", "subject", "Subject Id")
    table.load_data(observations_csv)

    assert table.get_rows("no-such-subject") == []


def test_load_data_raises_when_join_column_missing(tmp_path):
    path = tmp_path / "observations.csv"
    path.write_text("Other Col,Value\nX,1\n")

    table = EmbedableTable("observations", "subject", "Subject Id")
    with pytest.raises(AssertionError):
        table.load_data(path)
