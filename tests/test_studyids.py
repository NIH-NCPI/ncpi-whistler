import json

import pytest

from wstlr.studyids import StudyIDs


@pytest.fixture
def ids_file(tmp_path):
    return tmp_path / "study-ids.json"


class TestDumpToFile:
    def test_creates_file_with_nested_host_and_resource_structure(self, ids_file):
        ids = StudyIDs("https://fhir.example.org", study_id="STUDY1")
        ids.add_id("Patient", "p1")
        ids.add_id("Observation", "o1")
        ids.dump_to_file(ids_file)

        data = json.loads(ids_file.read_text())
        assert data == {
            "STUDY1": {
                "https://fhir.example.org": {
                    "Patient": ["p1"],
                    "Observation": ["o1"],
                }
            }
        }

    def test_deduplicates_and_sorts_ids(self, ids_file):
        ids = StudyIDs("https://fhir.example.org", study_id="STUDY1")
        for id_ in ["p2", "p1", "p1", "p3", "p2"]:
            ids.add_id("Patient", id_)
        ids.dump_to_file(ids_file)

        data = json.loads(ids_file.read_text())
        assert data["STUDY1"]["https://fhir.example.org"]["Patient"] == [
            "p1",
            "p2",
            "p3",
        ]

    def test_merges_with_existing_studies_in_the_file_rather_than_overwriting(
        self, ids_file
    ):
        first = StudyIDs("https://fhir.example.org", study_id="STUDY1")
        first.add_id("Patient", "p1")
        first.dump_to_file(ids_file)

        second = StudyIDs("https://fhir.example.org", study_id="STUDY2")
        second.add_id("Patient", "q1")
        second.dump_to_file(ids_file)

        data = json.loads(ids_file.read_text())
        assert set(data.keys()) == {"STUDY1", "STUDY2"}
        assert data["STUDY1"]["https://fhir.example.org"]["Patient"] == ["p1"]
        assert data["STUDY2"]["https://fhir.example.org"]["Patient"] == ["q1"]


class TestLoadFromFile:
    def test_returns_empty_list_when_file_does_not_exist_yet(self, ids_file):
        ids = StudyIDs("https://fhir.example.org", study_id="STUDY1")
        assert ids.load_from_file(ids_file) == []

    def test_returns_studies_present_for_the_matching_host(self, ids_file):
        writer = StudyIDs("https://fhir.example.org", study_id="STUDY1")
        writer.add_id("Patient", "p1")
        writer.dump_to_file(ids_file)

        other_host_writer = StudyIDs("https://other.example.org", study_id="STUDY2")
        other_host_writer.add_id("Patient", "q1")
        other_host_writer.dump_to_file(ids_file)

        reader = StudyIDs("https://fhir.example.org")
        studies = reader.load_from_file(ids_file)

        assert studies == ["STUDY1"]

    def test_get_ids_and_list_resource_types_after_loading(self, ids_file):
        writer = StudyIDs("https://fhir.example.org", study_id="STUDY1")
        writer.add_id("Patient", "p1")
        writer.add_id("Observation", "o1")
        writer.dump_to_file(ids_file)

        reader = StudyIDs("https://fhir.example.org")
        reader.load_from_file(ids_file)

        assert reader.get_ids("STUDY1", "Patient") == ["p1"]
        assert sorted(reader.list_resource_types("STUDY1")) == [
            "Observation",
            "Patient",
        ]
