from django.apps import AppConfig

from core.rights_declaration import RightsDeclaration

MODULE_NAME = "claim_batch"


# Droits, par entite puis par action.
#
# `relativeIndex.query` partage 111102 avec la lecture des traitements par lot : les
# index relatifs sont une vue sur ces traitements, pas une entite a droit propre - ce
# droit valait [] jusqu'ici, donc ouvert a tous.
#
# `capitationPaymentReport` porte 131218, identifiant du module report pour le meme
# etat : une reutilisation, pas une collision.
DJANGO_PERMS = {
    "batchRun": {
        "query": ("claim_batch.view_batchrun", 111102),
        "process": ("claim_batch.process_batchrun", 111101),
        "accountPreview": ("claim_batch.preview_account", 111103),
    },
    "relativeIndex": {
        "query": ("claim_batch.view_relativeindex", 111102),
    },
    "capitationPaymentReport": {
        "query": ("claim_batch.view_capitationpaymentreport", 131218),
    },
}

_PERM_CFG = {
    "gql_query_batch_runs_perms": ("batchRun", "query"),
    "gql_mutation_process_batch_perms": ("batchRun", "process"),
    "account_preview_perms": ("batchRun", "accountPreview"),
    "gql_query_relative_indexes_perms": ("relativeIndex", "query"),
    "reports_capitation_payment_perms": ("capitationPaymentReport", "query"),
}

RIGHTS = RightsDeclaration(MODULE_NAME, DJANGO_PERMS, _PERM_CFG)

perms = RIGHTS.perms
django_perms = RIGHTS.django_perm_names
configured_perms = RIGHTS.configured
require = RIGHTS.require


DEFAULT_CFG = {
    # Was [] - and `has_perms([])` returns True, so this query was open to every
    # authenticated user. Aliased onto gql_query_batch_runs_perms (111102), the read right for the
    # entity it belongs to: no new id and no role to grant, and it narrows the
    # query from everyone to that entity's readers. A dedicated id would narrow it
    # further and is the better end state.
}


class ClaimBatchConfig(AppConfig):
    name = MODULE_NAME

    # Droits: constantes, plus surchargeables. Ils ne passent plus par le
    # DEFAULT_CFG ni par ready(): `ModuleConfiguration.get_or_default` ignore
    # desormais toute cle `_perms` stockee en base.
    gql_query_batch_runs_perms = RIGHTS.perms("batchRun", "query")
    gql_query_relative_indexes_perms = RIGHTS.perms("relativeIndex", "query")
    gql_mutation_process_batch_perms = RIGHTS.perms("batchRun", "process")
    reports_capitation_payment_perms = RIGHTS.perms("capitationPaymentReport", "query")
    account_preview_perms = RIGHTS.perms("batchRun", "accountPreview")

    def __load_config(self, cfg):
        for field in cfg:
            if hasattr(ClaimBatchConfig, field):
                setattr(ClaimBatchConfig, field, cfg[field])

    def ready(self):
        from core.models import ModuleConfiguration
        cfg = ModuleConfiguration.get_or_default(MODULE_NAME, DEFAULT_CFG)
        self.__load_config(cfg)
