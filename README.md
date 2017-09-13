# Export Worker

[![Build Status](https://travis-ci.org/Cadasta/cadasta-export-worker.svg?branch=master)](https://travis-ci.org/Cadasta/cadasta-export-worker)
[![Requirements Status](https://requires.io/github/Cadasta/cadasta-export-worker/requirements.svg?branch=master)](https://requires.io/github/Cadasta/cadasta-export-worker/requirements/?branch=master)

* **Purpose**: Handles extraction and packaging of data exports.
* **Queue**: `export`

## Public Tasks

### `export.project`

Exports specified Cadasta Project into specified data formats. Results in a link to a [ZipStream](https://github.com/Cadasta/ZipStream) bundle containing generated files.

#### Signature

Argument | Description
--- | ---
`org_slug` | Slug identifier for Cadasta Organization.
`project_slug` | Slug identifier for Cadasta Project.
`api_key` | [DRF Temporary Scoped Token](https://github.com/Cadasta/drf-tmp-scoped-token)
`output_type` | Output format identifier. Currently supported: `'shp'` (export of all locations in shapefile format), `'res'` (export of all resource files, includes XLS spreadsheet of resource details), `'xls'` (export of all parties, locations, and relationships in a single XLS spreadsheet), `'all'` (export of all aforementioned files in a single bundle).
