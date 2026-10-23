from wstlr.module_summary import ModuleSummary


class TestSummary:
    def test_counts_resources_per_module_and_overall(self):
        summary = ModuleSummary()
        summary.summary("patients", {"resourceType": "Patient"})
        summary.summary("patients", {"resourceType": "Patient"})
        summary.summary("observations", {"resourceType": "Observation"})

        assert summary.module_summary["patients"]["Patient"] == 2
        assert summary.module_summary["observations"]["Observation"] == 1
        assert summary.resource_summary["Patient"] == 2
        assert summary.resource_summary["Observation"] == 1

    def test_restricting_to_resource_types_ignores_others(self):
        summary = ModuleSummary(resource_types=["Patient"])
        summary.summary("patients", {"resourceType": "Patient"})
        summary.summary("observations", {"resourceType": "Observation"})

        assert "Observation" not in summary.resource_summary
        assert summary.resource_summary["Patient"] == 1
        assert "observations" not in summary.module_summary

    def test_unrestricted_summary_starts_with_no_counts(self):
        summary = ModuleSummary()
        assert dict(summary.resource_summary) == {}
        assert dict(summary.module_summary) == {}
