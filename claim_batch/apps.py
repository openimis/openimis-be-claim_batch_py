from django.apps import AppConfig

MODULE_NAME = "claim_batch"

DEFAULT_CFG = {
    "gql_query_batch_runs_perms": [],
    "gql_query_relative_indexes_perms": [],
    "gql_mutation_process_batch_perms": ["111101"],
    "account_preview_perms": ["111103"]
}


class ClaimBatchConfig(AppConfig):
    name = MODULE_NAME

    gql_query_batch_runs_perms = []
    gql_query_relative_indexes_perms = []
    gql_mutation_process_batch_perms = []
    account_preview_perms = []

    def _configure_permissions(self, cfg):
        ClaimBatchConfig.gql_query_batch_runs_perms = cfg[
            "gql_query_batch_runs_perms"]
        ClaimBatchConfig.gql_query_relative_indexes_perms = cfg[
            "gql_query_relative_indexes_perms"]
        ClaimBatchConfig.account_preview_perms = cfg[
            "account_preview_perms"]

    def ready(self):
        from core.models import ModuleConfiguration
        cfg = ModuleConfiguration.get_or_default(MODULE_NAME, DEFAULT_CFG)
        self._configure_permissions(cfg)
