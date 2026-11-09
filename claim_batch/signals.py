"""
Signals for claim_batch.

batch_run_determine_products:
    Sent when a BatchRun's scope is neither a Product nor a Location (or when no products
    can be derived from the scope). Receivers can populate the product list.

    Providing args:
        batch_run: the BatchRun instance

batch_run_register_scope_model:
    Sent to discover additional models that can be used as the target of BatchRun.scope
    (GenericForeignKey). This allows dynamic support for scope__* GraphQL filters and
    validation without hardcoding in claim_batch.

    Receivers should populate the provided registry dict:

        registry[MyModelClass] = MyGQLTypeClass

    Core Location and Product are always included; receivers add more.

    The signal is fired both when building the schema (to declare scope__* args)
    and at runtime when applying filters for a specific scope_type.
"""
from django.dispatch import Signal

batch_run_determine_products = Signal()
batch_run_register_scope_model = Signal()
