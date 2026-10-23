import pytest

from wstlr.dd.variable import DdVariable


def make_variable(**overrides):
    kwargs = {
        "study_name": "MyStudy",
        "table_name": "demographics",
        "variable_name": "sex",
    }
    kwargs.update(overrides)
    return DdVariable(**kwargs)


class TestParseDataType:
    def test_unrecognized_type_terminates(self):
        with pytest.raises(SystemExit) as excinfo:
            make_variable(data_type="not-a-real-type")
        assert excinfo.value.code == 1

    def test_recognized_type_is_standardized(self):
        var = make_variable(data_type="Integer")
        assert var.data_type == "int"

    def test_defaults_to_string(self):
        var = make_variable()
        assert var.data_type == "string"


class TestParseEnums:
    def test_none_yields_no_values(self):
        var = make_variable()
        assert var.values_for_json() == []

    def test_semicolon_delimited_code_equals_desc(self):
        var = make_variable(data_type="enumeration", enumerations="M=Male;F=Female")
        assert var.values_for_json() == [
            {"code": "M", "description": "Male"},
            {"code": "F", "description": "Female"},
        ]

    def test_newline_delimited_bare_values_use_value_as_own_description(self):
        var = make_variable(
            data_type="enumeration", enumerations="White\nBlack\nAsian"
        )
        assert var.values_for_json() == [
            {"code": "White", "description": "White"},
            {"code": "Black", "description": "Black"},
            {"code": "Asian", "description": "Asian"},
        ]

    def test_accepts_a_list_instead_of_a_delimited_string(self):
        var = make_variable(data_type="enumeration", enumerations=["A=Alpha", "B=Beta"])
        assert var.values_for_json() == [
            {"code": "A", "description": "Alpha"},
            {"code": "B", "description": "Beta"},
        ]

    def test_duplicate_codes_keep_first_occurrence(self):
        var = make_variable(
            data_type="enumeration", enumerations="A=Alpha;A=Alternate"
        )
        assert var.values_for_json() == [{"code": "A", "description": "Alpha"}]


class TestDesc:
    def test_uses_description_when_present(self):
        var = make_variable(description="Biological sex")
        assert var.desc == "Biological sex"

    def test_falls_back_to_varname_when_description_blank(self):
        var = make_variable(description="   ")
        assert var.desc == "sex"


class TestAddToVarnameLookup:
    def test_maps_description_and_enumerations_back_to_codes(self):
        var = make_variable(
            description="Biological sex",
            data_type="enumeration",
            enumerations="M=Male;F=Female",
        )
        lookup = {}
        var.add_to_varname_lookup(lookup)
        assert lookup == {
            "Biological sex": "sex",
            "sex:Male": "M",
            "sex:Female": "F",
        }

    def test_skips_description_entry_when_description_equals_varname(self):
        var = make_variable(description="sex")
        lookup = {}
        var.add_to_varname_lookup(lookup)
        assert lookup == {}


class TestObjAsCs:
    def test_includes_url_study_and_values(self):
        var = make_variable(
            data_type="enumeration", enumerations="M=Male;F=Female"
        )
        obj = var.obj_as_cs()
        assert obj["varname"] == "sex"
        assert obj["study"] == "MyStudy"
        assert obj["table_name"] == "demographics"
        assert obj["url"].endswith("/demographics/sex")
        assert obj["values"] == [
            {"code": "M", "description": "Male"},
            {"code": "F", "description": "Female"},
        ]

    def test_consent_group_is_included_in_obj_and_scoped_into_the_url(self):
        var = make_variable(consent_group="GRU")
        assert var.obj_as_cs()["consent_group"] == "GRU"
        assert "/gru/" in var.url

    def test_different_consent_groups_produce_different_urls(self):
        gru = make_variable(consent_group="GRU").url
        hmb = make_variable(consent_group="HMB").url
        assert gru != hmb

    def test_consent_group_is_omitted_when_absent(self):
        var = make_variable()
        assert "consent_group" not in var.obj_as_cs()
