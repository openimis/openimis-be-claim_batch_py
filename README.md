# openIMIS Backend Claim Batch reference module
This repository holds the files of the openIMIS Backend Claim Batch reference module.
It is dedicated to be deployed as a module of [openimis-be_py](https://github.com/openimis/openimis-be_py).

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)

## Code climat (develop branch)

## ORM mapping:
* tblBatchRun > BatchRun
* tblRelIndex > RelativeIndex

## Listened Django Signals
None

## Services
* ProcessBatchService,
* ReportDataService, loading the necessary data for the various reports (pbh, pbp, pbc_H and pbc_P) - WARNING - Today bound to uspSSRSProcessBatchWithClaim and uspSSRSProcessBatch stored procedures, will be migrated to python code.

## Reports (template can be overloaded via report.ReportDefinition)
* pbh: batch results preview, group by health facility
* pbp: batch results preview, group by  product
* pbc_H: batch results preview with claims, group by health facility
* pbc_P: batch results preview with claims, group by product

## GraphQL Queries
* batch_runs
* batch_runs_summaries

## GraphQL Mutations - each mutation emits default signals and return standard error lists (cfr. openimis-be-core_py)
* process_batch - WARNING - Today bound to uspBatchProcess Stored Procedure, will be migrated to python code

## Additional Endpoints
* report: generating preview PDF

## Reports
* `claim_batch_pbc_P`: Account preview, with claims, group by products
* `claim_batch_pbc_H`: Account preview, with claims, group by health facilities
* `claim_batch_pbh`: Account preview, group by products
* `claim_batch_pbp`: Account preview, group by products

## Configuration options (can be changed via core.ModuleConfiguration)
* gql_query_batch_runs_perms: required rights to call batch_runs GraphQL query (default: [])
* gql_query_relative_indexes_perms: required rights to call relative_indexes GraphQL query (default: [])
* gql_mutation_process_batch_perms: required rights to call process_batch GraphQL query (default: ["111101"])
* account_preview_perms: required rights to call account_preview end point (default: ["111103"])

## openIMIS Modules Dependencies
* location.models.Location
* product.models.Product
