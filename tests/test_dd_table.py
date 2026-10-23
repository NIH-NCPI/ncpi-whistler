import pytest

from wstlr.dd.table import DdTable


@pytest.fixture(autouse=True)
def restore_default_subject_id():
    """DdTable.default_subject_id() mutates a module-level global, so tests
    that touch it must not leak state into other tests."""
    original = DdTable.default_subject_id()
    yield
    DdTable.default_subject_id(original)


class TestConstruction:
    def test_uses_class_default_subject_id_when_not_specified(self):
        table = DdTable("demographics", "MyStudy")
        assert table.subject_id == "subject_id"

    def test_explicit_subject_id_overrides_default(self):
        table = DdTable("demographics", "MyStudy", subject_id="participant_id")
        assert table.subject_id == "participant_id"

    def test_changing_the_default_affects_subsequently_created_tables(self):
        DdTable.default_subject_id("custom_id")
        table = DdTable("demographics", "MyStudy")
        assert table.subject_id == "custom_id"

    def test_study_component_without_consent_group(self):
        table = DdTable("demographics", "MyStudy")
        assert table.study_component == "MyStudy"

    def test_study_component_includes_consent_group_when_present(self):
        table = DdTable("demographics", "MyStudy", consent_group="GRU")
        assert table.study_component == "MyStudy-GRU"

    def test_blank_consent_group_is_treated_as_absent(self):
        table = DdTable("demographics", "MyStudy", consent_group="   ")
        assert table.study_component == "MyStudy"

    def test_desc_falls_back_to_name_when_no_description_given(self):
        table = DdTable("demographics", "MyStudy")
        assert table.desc == "demographics"

    def test_desc_uses_description_when_given(self):
        table = DdTable("demographics", "MyStudy", description="Demographics table")
        assert table.desc == "Demographics table"

    def test_url_is_built_from_table_name(self):
        table = DdTable("demographics", "MyStudy")
        assert table.url.endswith("/data-dictionary/demographics")

    def test_url_is_unaffected_when_no_consent_group_is_given(self):
        without_group = DdTable("demographics", "MyStudy").url
        assert without_group.endswith("/data-dictionary/demographics")

    def test_consent_group_is_scoped_into_the_url(self):
        table = DdTable("demographics", "MyStudy", consent_group="GRU")
        assert table.url.endswith("/data-dictionary/gru/demographics")

    def test_different_consent_groups_produce_different_urls(self):
        gru = DdTable("demographics", "MyStudy", consent_group="GRU").url
        hmb = DdTable("demographics", "MyStudy", consent_group="HMB").url
        assert gru != hmb

    def test_blank_consent_group_does_not_affect_the_url(self):
        without_group = DdTable("demographics", "MyStudy").url
        blank_group = DdTable("demographics", "MyStudy", consent_group="   ").url
        assert blank_group == without_group


class TestAddVariable:
    def test_adds_variable_by_name(self):
        table = DdTable("demographics", "MyStudy")
        table.add_variable(variable_name="sex", data_type="string")
        assert "sex" in table.variables

    def test_key_component_variables_are_tracked_in_key(self):
        table = DdTable("demographics", "MyStudy")
        table.add_variable(variable_name="sex", data_type="string", key_component=True)
        table.add_variable(variable_name="notes", data_type="string")
        assert table.key == ["sex"]

    def test_duplicate_variable_name_terminates(self):
        table = DdTable("demographics", "MyStudy")
        table.add_variable(variable_name="sex", data_type="string")
        with pytest.raises(SystemExit) as excinfo:
            table.add_variable(variable_name="sex", data_type="string")
        assert excinfo.value.code == 1


class TestSerialization:
    def _table_with_sex_variable(self):
        table = DdTable("demographics", "MyStudy")
        table.add_variable(
            variable_name="sex",
            data_type="enumeration",
            enumerations="M=Male;F=Female",
        )
        return table

    def test_obj_as_cs_lists_variable_codes(self):
        table = self._table_with_sex_variable()
        obj = table.obj_as_cs()
        assert obj["table_name"] == "demographics"
        assert obj["study"] == "MyStudy"
        assert obj["values"] == [{"code": "sex", "description": "sex"}]

    def test_obj_as_dd_table_nests_each_variables_enumerated_values(self):
        table = self._table_with_sex_variable()
        obj = table.obj_as_dd_table()
        assert obj["table_name"] == "demographics"
        assert len(obj["variables"]) == 1
        assert obj["variables"][0]["varname"] == "sex"
        assert obj["variables"][0]["values"] == [
            {"code": "M", "description": "Male"},
            {"code": "F", "description": "Female"},
        ]

    def test_variables_as_cs_returns_one_entry_per_variable(self):
        table = self._table_with_sex_variable()
        table.add_variable(variable_name="notes", data_type="string")
        codesystems = table.variables_as_cs()
        assert sorted(cs["varname"] for cs in codesystems) == ["notes", "sex"]
