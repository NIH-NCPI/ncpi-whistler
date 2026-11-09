cwlVersion: v1.2
class: CommandLineTool
label: NCPI Whistler (play)
doc: |
  Runs the NCPI Whistler pipeline (`play`) on a Whistler project: builds the
  harmony ConceptMaps, extracts CSV to JSON, runs Google Whistle and, if a
  FHIR host is selected via `play_args` (e.g. ["--host", "dev"]), loads the
  resulting resources into that FHIR server.

requirements:
  DockerRequirement:
    # Replace with the image you pushed, e.g.
    # cavatica-images.sbgenomics.com/<user-or-division>/ncpi-whistler:<tag>
    dockerPull: cavatica-images.sbgenomics.com/torstees/ncpi-whistler:0.1.0
  ResourceRequirement:
    coresMin: 2
    ramMin: 8000
    tmpdirMin: 20000
  NetworkAccess:
    # Needed only when loading into a FHIR server.
    networkAccess: true

baseCommand: [whistler-run]

inputs:
  project_dir:
    type: Directory
    label: Whistler project folder
    doc: Folder containing the study YAML, data CSVs, harmony CSVs and Whistle projections.
    inputBinding:
      prefix: --project
  config:
    type: string
    label: Study config
    doc: Path of the study YAML, relative to the project folder (e.g. "my_study.yaml").
    inputBinding:
      prefix: --config
  fhir_hosts:
    type: File?
    label: fhir_hosts file
    doc: Required only when loading into a FHIR server.
    inputBinding:
      prefix: --hosts
  secrets:
    type:
      - "null"
      - type: array
        items: File
        inputBinding:
          prefix: --secret
    label: Credential files
    doc: Files referenced by fhir_hosts (e.g. a GCP service-account JSON), placed in the project root.
  play_args:
    type: string[]?
    label: Extra play arguments
    doc: Passed through to `play`, e.g. ["--host", "dev"] or ["--projection", "harmonized"].
    inputBinding:
      position: 100
      prefix: --

stdout: whistler.log
stderr: whistler.err.log

outputs:
  results:
    type: Directory
    doc: Everything collected from the run (output/ and harmony ConceptMaps).
    outputBinding:
      glob: results
  whistle_output:
    type: File[]
    doc: The Whistle-generated FHIR JSON files.
    outputBinding:
      glob: results/output/whistle-output/*/*.output.json
  log:
    type: stdout
  error_log:
    type: stderr
