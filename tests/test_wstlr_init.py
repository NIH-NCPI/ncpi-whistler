import pytest

from wstlr import (
    InvalidType,
    StandardizeDdType,
    TableType,
    clean_values,
    dd_system_url,
    determine_table_type,
    die_if,
    evaluate_bool,
    fix_fieldname,
)


class TestStandardizeDdType:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("string", "string"),
            ("Str", "string"),
            ("", "string"),
            ("identifier", "string"),
            ("int", "int"),
            ("Integer", "int"),
            ("bool", "boolean"),
            ("BOOLEAN", "boolean"),
            ("number", "number"),
            ("Decimal", "number"),
            ("float", "number"),
            ("enumeration", "enumeration"),
            ("date", "date"),
        ],
    )
    def test_known_types_map_to_canonical_form(self, raw, expected):
        assert StandardizeDdType(raw) == expected

    def test_unrecognized_type_raises_invalid_type(self):
        with pytest.raises(InvalidType) as excinfo:
            StandardizeDdType("not-a-real-type")
        assert excinfo.value.type_name == "not-a-real-type"
        assert "not-a-real-type" in excinfo.value.message()
        assert "not-a-real-type" in str(excinfo.value)


class TestDetermineTableType:
    def test_embed_key_means_embedded(self):
        assert determine_table_type({"embed": {"dataset": "subject"}}) == TableType.Embedded

    def test_group_by_key_means_grouped(self):
        assert determine_table_type({"group_by": "subject_id"}) == TableType.Grouped

    def test_neither_key_means_default(self):
        assert determine_table_type({"filename": "table.csv"}) == TableType.Default

    def test_embed_takes_priority_over_group_by(self):
        table_def = {"embed": {"dataset": "subject"}, "group_by": "subject_id"}
        assert determine_table_type(table_def) == TableType.Embedded


class TestCleanValues:
    def test_none_returns_empty_string(self):
        assert clean_values(None) == ""

    def test_collapses_semicolon_whitespace(self):
        assert clean_values("a;   b;    c") == "a;b;c"

    def test_strips_surrounding_whitespace(self):
        assert clean_values("  a; b  ") == "a;b"


class TestFixFieldname:
    def test_lowercases_and_trims(self):
        assert fix_fieldname("  My Column  ") == "my_column"

    def test_strips_parens_and_slashes(self):
        assert fix_fieldname("Value (Units)/Test") == "value_units_test"


class TestDdSystemUrl:
    def test_without_varname_omits_variable_segment(self):
        url = dd_system_url("http://base", "term", None, "My Table", None)
        assert url == "http://base/term/data-dictionary/my_table"

    def test_with_varname_appends_variable_segment(self):
        url = dd_system_url("http://base", "term", None, "My Table", "My Var")
        assert url == "http://base/term/data-dictionary/my_table/my_var"

    def test_consent_group_is_inserted_before_table_name(self):
        url = dd_system_url("http://base", "term", "GRU", "My Table", None)
        assert url == "http://base/term/data-dictionary/gru/my_table"

    def test_consent_group_carries_through_to_variable_urls(self):
        url = dd_system_url("http://base", "term", "GRU", "My Table", "My Var")
        assert url == "http://base/term/data-dictionary/gru/my_table/my_var"

    def test_blank_consent_group_is_treated_as_absent(self):
        url = dd_system_url("http://base", "term", "   ", "My Table", None)
        assert url == "http://base/term/data-dictionary/my_table"

    def test_different_consent_groups_produce_different_urls(self):
        gru_url = dd_system_url("http://base", "term", "GRU", "My Table", None)
        hmb_url = dd_system_url("http://base", "term", "HMB", "My Table", None)
        assert gru_url != hmb_url


class TestEvaluateBool:
    @pytest.mark.parametrize(
        "value,expected",
        [
            (True, True),
            (False, False),
            ("true", True),
            ("YES", True),
            ("1", True),
            (1, True),
            ("no", False),
            ("false", False),
            (0, False),
            (None, False),
            ("banana", False),
        ],
    )
    def test_evaluates_common_truthy_and_falsy_forms(self, value, expected):
        assert evaluate_bool(value) is expected


class TestDieIf:
    def test_exits_with_given_errnum_when_condition_true(self, capsys):
        with pytest.raises(SystemExit) as excinfo:
            die_if(True, "something went wrong", errnum=3)
        assert excinfo.value.code == 3
        assert "something went wrong" in capsys.readouterr().err

    def test_does_nothing_when_condition_false(self, capsys):
        die_if(False, "should not be printed")
        assert capsys.readouterr().err == ""
