# Changelog

## November 2024 — Breaking Changes (0.3.0)
Version 0.3.0 introduced breaking changes to the YAML configuration: projections are no longer modular and require entries for each of the following: `study`, `dataset`, `harmonized`. Inside, you can create a directory, `current` (the default choice for projection version), and move your projections into that directory. You can then create an entry for each of the modules that point to the same projection library:

```yaml
projections:
  study: projector
  dataset: projector
  harmonized: projector
```

You should move your entry point (typically `_entry.wstl`) into the `projector` directory (Whistler will look to the version's parent directory for the file).
