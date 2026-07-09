import pytest

from wstlr.hostfile import load_hosts_file


class TestLoadHostsFile:
    def test_parses_an_existing_yaml_file(self, tmp_path):
        hosts_file = tmp_path / "fhir_hosts"
        hosts_file.write_text(
            "dev:\n"
            "  host_desc: Dev\n"
            "  target_service_url: http://example.org/fhir\n"
            "  auth_type: auth_basic\n"
        )

        config = load_hosts_file(hosts_file)

        assert config == {
            "dev": {
                "host_desc": "Dev",
                "target_service_url": "http://example.org/fhir",
                "auth_type": "auth_basic",
            }
        }

    def test_missing_file_writes_example_config_and_exits(self, tmp_path, capsys):
        missing_file = tmp_path / "does-not-exist"

        with pytest.raises(SystemExit) as excinfo:
            load_hosts_file(missing_file)

        assert excinfo.value.code == 1
        captured = capsys.readouterr()
        assert "must exist in cwd" in captured.err
        assert "Example Hosts Configuration" in captured.out

    def test_empty_file_is_treated_as_missing(self, tmp_path):
        empty_file = tmp_path / "fhir_hosts"
        empty_file.write_text("")

        with pytest.raises(SystemExit):
            load_hosts_file(empty_file)
