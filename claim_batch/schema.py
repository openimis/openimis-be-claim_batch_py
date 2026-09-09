import hashlib
from collections import defaultdict

import graphene
from django.core.exceptions import PermissionDenied
from django.db.models import Q
from django.contrib.contenttypes.models import ContentType

from core import prefix_filterset, ExtendedConnection
from core.schema import OpenIMISMutation, OrderedDjangoFilterConnectionField
from graphene import ObjectType
from graphene_django import DjangoObjectType
from graphene_django.filter import DjangoFilterConnectionField
from product.schema import ProductGQLType
from location.schema import LocationGQLType
from location.models import Location
from product.models import Product

from .models import BatchRun, RelativeIndex
from .services import ProcessBatchSubmit, ProcessBatchService, _resolve_scope
from .apps import ClaimBatchConfig
from . import signals
from django.utils.translation import gettext as _


def _get_scope_model_registry():
    """Return the current mapping of scope target model classes -> GQLType classes.

    Starts with the core ones (Location, Product) using the same style of
    isinstance / direct matching logic that already exists for resolving
    products from scope (see get_products_for_batch_run in services.py,
    the scope property in models.py, and _resolve_scope).

    Then fires the batch_run_register_scope_model signal so other modules
    (or custom scope handlers) can register additional models + their GQLTypes.

    Receivers do:
        registry[TheirModelClass] = TheirGQLTypeClass

    This powers:
    - the unconstrained scope__* arguments in the batch_runs filter
    - runtime lookup of which GQLType's filter_fields to validate against
      and which concrete model to query for the scope_id subquery.
    """
    registry = {
        Location: LocationGQLType,
        Product: ProductGQLType,
    }
    signals.batch_run_register_scope_model.send(sender=BatchRun, registry=registry)
    return registry


def get_gql_type_for_scope(scope_obj):
    """Given a concrete scope instance (or None), return the associated GQLType
    using the same resolution style as the product-from-scope logic.
    Useful for resolvers or custom filter extensions.
    """
    if scope_obj is None:
        return None
    registry = _get_scope_model_registry()
    return registry.get(scope_obj.__class__)


def get_scope_target_filter_fields():
    """Build unconstrained scope__* filter arguments.

    IMPORTANT: We cannot pass raw filter_fields specs (lists like ["exact", "icontains"])
    directly as **kwargs to OrderedDjangoFilterConnectionField — graphene will try to
    sort the argument values and fail with " '<' not supported between instances of 'list' and 'List' ".

    Instead we generate proper graphene.String() (or List for 'in') argument definitions.
    The actual interpretation + validation against the target GQLType happens in the resolver.
    """
    import graphene

    registry = _get_scope_model_registry()
    args = {}

    for gql_type in registry.values():
        if not gql_type or not hasattr(gql_type, "_meta"):
            continue

        for field, lookups in gql_type._meta.filter_fields.items():
            # Avoid collisions with the direct scope_id / scope_type scalars
            # that are already declared on BatchRunGQLType.
            if field in ("id", "type"):
                continue

            for lookup in lookups:
                # Build GraphQL arg names like scope_Code, scope_Parent_Uuid, scope_Code_Icontains
                # We keep underscores for nested fields so the resolver's _arg_to_lookup
                # can reliably turn them back into Django lookups (parent__uuid etc.).
                base = field.replace('__', '_')
                if lookup == "exact":
                    arg_name = f"scope_{base}"
                else:
                    arg_name = f"scope_{base}_{lookup}"

                # Choose a reasonable graphene type.
                # The resolver will coerce/interpret the value when building the
                # target queryset (Location.objects.filter(...) or Product...).
                if lookup == "in":
                    args[arg_name] = graphene.List(graphene.String)
                else:
                    args[arg_name] = graphene.String()

    return args


class BatchRunGQLType(DjangoObjectType):
    # Scope is a GenericForeignKey (Location, Product, or custom via signal).
    scope = graphene.JSONString()
    scope_id = graphene.String()
    scope_type = graphene.String()
    scope_type_name = graphene.String()
    scope_type_model = graphene.String()

    # Products stored in json_ext or derived from scope
    products = graphene.List(ProductGQLType)

    class Meta:
        model = BatchRun
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "id": ["exact"],
            "run_date": ["exact", "lt", "lte", "gt", "gte"],
            "scope_id": ["exact"],
            "scope_type": ["exact"],
            # Note: rich filtering on scope targets uses unconstrained scope__* args
            # declared on the batch_runs field (via the register_scope_model signal).
            # Because scope is a GFK we manually convert using the target GQLType's fields
            # then restrict via scope_type + scope_id__in.
        }
        connection_class = ExtendedConnection

    @staticmethod
    def resolve_scope(root, info):
        if not info.context.user.has_perms(ClaimBatchConfig.gql_query_batch_runs_perms):
            raise PermissionDenied(_("unauthorized"))
        if root.scope:
            try:
                # Return a dict with the target object's data + scope metadata.
                # Graphene JSONString will serialize the dict.
                obj_dict = getattr(root.scope, '__dict__', {}).copy()
                obj_dict.pop('_state', None)
                obj_dict.pop('_prefetched_objects_cache', None)
                # Add scope context
                obj_dict['_scope_type'] = f"{root.scope_type.app_label}.{root.scope_type.model}" if root.scope_type else None
                obj_dict['_scope_id'] = root.scope_id
                return obj_dict
            except Exception:
                return {"_scope_id": root.scope_id, "_scope_type": str(root.scope_type)}
        return None

    @staticmethod
    def resolve_scope_id(root, info):
        if not info.context.user.has_perms(ClaimBatchConfig.gql_query_batch_runs_perms):
            raise PermissionDenied(_("unauthorized"))
        return root.scope_id

    @staticmethod
    def resolve_scope_type(root, info):
        if not info.context.user.has_perms(ClaimBatchConfig.gql_query_batch_runs_perms):
            raise PermissionDenied(_("unauthorized"))
        if root.scope_type:
            return root.scope_type.id
        return None

    @staticmethod
    def resolve_scope_type_name(root, info):
        if not info.context.user.has_perms(ClaimBatchConfig.gql_query_batch_runs_perms):
            raise PermissionDenied(_("unauthorized"))
        if root.scope_type:
            return root.scope_type.name
        return None

    @staticmethod
    def resolve_scope_type_model(root, info):
        if not info.context.user.has_perms(ClaimBatchConfig.gql_query_batch_runs_perms):
            raise PermissionDenied(_("unauthorized"))
        if root.scope_type:
            return f"{root.scope_type.app_label}.{root.scope_type.model}"
        return None

    @staticmethod
    def resolve_products(root, info):
        if not info.context.user.has_perms(ClaimBatchConfig.gql_query_batch_runs_perms):
            raise PermissionDenied(_("unauthorized"))
        try:
            return root.products or []
        except Exception:
            return []


class BatchRunSummaryGQLType(ObjectType):
    run_year = graphene.Int()
    run_month = graphene.Int()
    product_label = graphene.String()
    care_type = graphene.String()
    calc_date = graphene.String()
    index = graphene.Float()

    class Meta:
        interfaces = (graphene.relay.Node,)


def _summary_scope_key(obj):
    """Normalize GFK scope for matching BatchRun ↔ RelativeIndex (replaces Location join)."""
    scope_id = obj.scope_id
    if scope_id is None or scope_id == "":
        return (None, None)
    return (obj.scope_type_id, str(scope_id))


def batchRunSummaryFilter(**kwargs):
    """Build ORM Q filters for RelativeIndex and BatchRun summary queries.

    Replaces the former raw-SQL fragment builder. Scope filters use the GFK
    (scope_type + scope_id) instead of the removed LocationId columns.
    """
    rel_filters = Q()
    batch_filters = Q()

    if kwargs.get("accountType"):
        rel_filters &= Q(type=kwargs.get("accountType"))
    if kwargs.get("accountYear"):
        batch_filters &= Q(run_year=kwargs.get("accountYear"))
    if kwargs.get("accountMonth"):
        batch_filters &= Q(run_month=kwargs.get("accountMonth"))

    location_id = kwargs.get("accountDistrict") or kwargs.get("accountRegion")
    if location_id:
        location_ct = ContentType.objects.get_for_model(Location)
        scope_q = Q(scope_type=location_ct, scope_id=str(location_id))
        rel_filters &= scope_q
        batch_filters &= scope_q
    else:
        # National / null-scope runs (legacy LocationId IS NULL)
        null_scope = Q(scope_id__isnull=True) | Q(scope_id="")
        rel_filters &= null_scope
        batch_filters &= null_scope

    if kwargs.get("accountProduct"):
        rel_filters &= Q(product_id=kwargs.get("accountProduct"))
    if kwargs.get("accountCareType"):
        rel_filters &= Q(care_type=kwargs.get("accountCareType"))

    return rel_filters, batch_filters


class BatchRunSummaryConnection(graphene.Connection):
    class Meta:
        node = BatchRunSummaryGQLType

    total_count = graphene.Int()

    def resolve_total_count(self, info, **kwargs):
        return len(self.iterable)


class RelativeIndexGQLType(DjangoObjectType):
    class Meta:
        model = RelativeIndex
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "id": ["exact"],
            "period": ["exact"],
            "care_type": ["exact"],
            **prefix_filterset("product__", ProductGQLType._meta.filter_fields)
        }
        connection_class = ExtendedConnection


class ProcessBatchMutation(OpenIMISMutation):
    """
    Process Batch.
    """
    _mutation_module = "claim_batch"
    _mutation_class = "ProcessBatchMutation"

    class Input(OpenIMISMutation.Input):
        # Legacy support
        location_id = graphene.Int(required=False)
        # New scope (GFK) support.
        # scope_type can be "Location", "Product", "location.Location", "product.Product" or the ContentType id.
        scope_id = graphene.String(required=False)
        scope_type = graphene.String(required=False)
        year = graphene.Int()
        month = graphene.Int()

    @staticmethod
    def _resolve_scope_model(scope_type_value):
        """Resolve the Python model class from a scope_type value.
        Accepts:
          - "Location", "Product"
          - "location.Location", "product.Product"
          - ContentType primary key (as int or str)
        """
        if not scope_type_value:
            return None
        try:
            val = str(scope_type_value).strip()
            ct = None
            if val.isdigit():
                ct = ContentType.objects.filter(pk=int(val)).first()
            else:
                class_name = val
                if '.' in class_name:
                    class_name = class_name.split('.')[-1]
                ct = ContentType.objects.filter(model__iexact=class_name).first()
                if ct is None:
                    # fallback: try matching by app_label too
                    ct = ContentType.objects.filter(
                        model__iexact=class_name
                    ).first()
            return ct.model_class() if ct else None
        except Exception:
            return None

    @classmethod
    def async_mutate(cls, user, **data):
        if not user.has_perms(ClaimBatchConfig.gql_mutation_process_batch_perms):
            raise PermissionDenied(_("unauthorized"))

        scope = None
        scope_id = data.get("scope_id") or data.get("location_id")
        scope_type_value = data.get("scope_type")

        if scope_id:
            model_class = cls._resolve_scope_model(scope_type_value)
            if model_class:
                try:
                    scope = model_class.objects.get(pk=scope_id)
                except model_class.DoesNotExist:
                    scope = None
            if scope is None:
                # Final legacy fallback (old location id behavior)
                scope = _resolve_scope(scope_id)

        submit = ProcessBatchSubmit(
            location_id=data.get("location_id"),
            scope=scope,
            year=data["year"],
            month=data["month"],
        )
        service = ProcessBatchService(user)
        res = service.submit(submit)
        return res


class Query(graphene.ObjectType):
    batch_runs = OrderedDjangoFilterConnectionField(
        BatchRunGQLType,
        orderBy=graphene.List(of_type=graphene.String),
        # Unconstrained scope__* filters (scope_Code, scope_Name_Icontains, etc.).
        # We generate proper graphene argument objects here (not raw filter specs)
        # so that graphene can sort/process the extra args without error.
        # Runtime interpretation + lookup of the correct target GQLType (via signal)
        # happens in resolve_batch_runs / _apply_scope_filters.
        **get_scope_target_filter_fields(),
    )
    batch_runs_summaries = graphene.relay.ConnectionField(
        BatchRunSummaryConnection,
        accountType=graphene.Int(),
        accountYear=graphene.Int(),
        accountMonth=graphene.Int(),
        accountRegion=graphene.Int(),
        accountDistrict=graphene.Int(),
        accountProduct=graphene.Int(),
        accountCareType=graphene.String()
    )
    relative_indexes = DjangoFilterConnectionField(RelativeIndexGQLType)

    @staticmethod
    def _apply_scope_filters(qs, kwargs):
        """
        Support unconstrained scope_* filters (e.g. scope_Code, scope_Name_Icontains).

        Resolution strategy:
        - Collect any scope__* / scope_* args (excluding the direct scalar scope_id/scope_type).
        - Look at the accompanying scope_type (or scope_Type) value (if any) to determine
          which model is the scope target.
        - Use the registry (populated via batch_run_register_scope_model signal,
          using the same style of scope model resolution already present for products)
          to get the GQLType that manages that model class.
        - Validate (at least the base field names) against that GQLType's filter_fields.
        - Build a subquery on the concrete target model and restrict via
          scope_type + scope_id__in .

        This way the same `scope_Code` arg works for whatever the current scope_type is
        (Location today, Product tomorrow, or custom via future registration).
        """
        if not kwargs:
            return qs

        # Collect scope target filters (unconstrained scope_ prefix)
        # Exclude the direct scalar filters (scope_id, scope_type) handled by BatchRunGQLType.
        scope_target_args = {}
        for k, v in kwargs.items():
            kl = k.lower()
            if kl.startswith("scope_"):
                after = kl[6:]  # after "scope_"
                # skip direct scalars and their lookup variants
                if after and not after.startswith(("id", "type")):
                    scope_target_args[k] = v

        # scope isnull (no scope assigned)
        for k, v in list(kwargs.items()):
            if k.lower() in ("scope_isnull", "scope__isnull"):
                if v:
                    qs = qs.filter(Q(scope_id__isnull=True) | Q(scope_id=""))
                else:
                    qs = qs.exclude(Q(scope_id__isnull=True) | Q(scope_id=""))

        if not scope_target_args:
            return qs

        # Determine the target model + GQLType by inspecting the scope_type filter value
        st_val = (
            kwargs.get("scope_type")
            or kwargs.get("scope_Type")
            or kwargs.get("scopeType")
            or kwargs.get("scope_type__exact")
        )
        target_model, gql_type = Query._resolve_scope_target(st_val)

        if target_model is None or gql_type is None:
            # No scope_type info provided together with the rich filters.
            # Fallback to Location for backward compatibility with existing queries
            # that were written against the old location__ behavior.
            target_model = Location
            gql_type = LocationGQLType

        # Validate provided scope_ args against the chosen GQLType's filter_fields
        valid_base_fields = set(gql_type._meta.filter_fields.keys())
        validated_args = {}
        for arg, val in scope_target_args.items():
            # arg is like "scope_Code" or "scope_Name_Icontains" -> base "code" or "name"
            base = arg.split("_", 1)[1] if "_" in arg else arg
            base_field = base.split("_")[0].lower() if "_" in base else base.lower()
            # Accept if the base field (or a prefix match) is declared in the GQLType
            if any(base_field == f.split("__")[0].lower() for f in valid_base_fields) or not valid_base_fields:
                validated_args[arg] = val
            # else: silently ignore unknown for this scope type (or we could raise)

        if validated_args:
            qs = Query._filter_by_target_scope(qs, validated_args, target_model, "scope_")

        return qs

    @staticmethod
    def _resolve_scope_target(scope_type_value):
        """Given a scope_type filter value, return (model_class, GQLType) or (None, None).

        Uses the dynamic registry populated via the batch_run_register_scope_model
        signal (plus core Location/Product matching logic).
        """
        if not scope_type_value:
            return None, None
        model_class = ProcessBatchMutation._resolve_scope_model(scope_type_value)
        if not model_class:
            return None, None
        registry = _get_scope_model_registry()
        gql_type = registry.get(model_class)
        return model_class, gql_type

    @staticmethod
    def _filter_by_target_scope(qs, args, target_model, prefix):
        """Apply the collected scope_* args as filters on the concrete target_model,
        then restrict the BatchRun queryset using the GFK columns (scope_type + scope_id).
        """
        def _arg_to_lookup(arg_name, value):
            # arg_name e.g. "scope_Code_Icontains" , prefix="scope_"
            if arg_name.lower().startswith(prefix):
                raw = arg_name[len(prefix):]
            else:
                raw = arg_name
            tokens = raw.split("_")

            LOOKUP_SUFFIXES = {
                "exact", "iexact", "contains", "icontains", "in", "gt", "gte", "lt", "lte",
                "startswith", "istartswith", "endswith", "iendswith", "isnull", "regex", "iregex", "ne",
            }

            lookup = "exact"
            is_ne = False
            if len(tokens) >= 2:
                last = tokens[-1].lower()
                if last in LOOKUP_SUFFIXES:
                    lookup = last
                    tokens = tokens[:-1]
                    if lookup == "ne":
                        is_ne = True
                        lookup = "exact"

            field_path = "__".join(t.lower() for t in tokens)
            if not field_path:
                return None

            expr = field_path if lookup == "exact" else f"{field_path}__{lookup}"
            return expr, value, is_ne

        django_filters = {}
        exclude_filters = []
        for arg_name, val in args.items():
            res = _arg_to_lookup(arg_name, val)
            if not res:
                continue
            expr, val, is_ne = res
            if is_ne:
                exclude_filters.append((expr, val))
            else:
                django_filters[expr] = val

        if not django_filters and not exclude_filters:
            return qs

        try:
            target_ct = ContentType.objects.get_for_model(target_model)
            target_qs = target_model.objects.filter(**django_filters)
            for expr, val in exclude_filters:
                target_qs = target_qs.exclude(**{expr: val})

            matched_ids = [str(pk) for pk in target_qs.values_list("id", flat=True)]
            if matched_ids:
                qs = qs.filter(scope_type=target_ct, scope_id__in=matched_ids)
            else:
                qs = qs.none()
        except Exception:
            qs = qs.none()

        return qs

    def resolve_batch_runs(self, info, **kwargs):
        if not info.context.user.has_perms(ClaimBatchConfig.gql_query_batch_runs_perms):
            raise PermissionDenied(_("unauthorized"))

        # Base queryset.
        # The normal filter_fields (scope_id, scope_type, id, run_date, ...) are applied
        # on top by the OrderedDjangoFilterConnectionField.
        #
        # location__* and product__* are handled here by turning them into
        # scope_type + scope_id__in conditions (because scope is a GFK).
        qs = BatchRun.objects.all()
        qs = self._apply_scope_filters(qs, kwargs)
        return qs

    def resolve_batch_runs_summaries(self, info, **kwargs):
        if not info.context.user.has_perms(ClaimBatchConfig.gql_query_batch_runs_perms):
            raise PermissionDenied(_("unauthorized"))

        rel_filters, batch_filters = batchRunSummaryFilter(**kwargs)

        # BatchRun and RelativeIndex are no longer joined via Location;
        # match on shared GFK scope (scope_type + scope_id), product via FK.
        batch_runs = list(
            BatchRun.objects.filter(batch_filters).order_by("run_year", "run_month")
        )
        if not batch_runs:
            return []

        by_scope = defaultdict(list)
        for br in batch_runs:
            by_scope[_summary_scope_key(br)].append(br)

        rel_indexes = (
            RelativeIndex.objects.filter(rel_filters)
            .select_related("product")
            .order_by("id")
        )

        res = []
        for ri in rel_indexes:
            product = ri.product
            product_id = product.id if product else ""
            product_label = (
                f"{product.code} {product.name}" if product else ""
            )
            calc_date = (
                ri.calc_date.strftime("%Y-%m-%d") if ri.calc_date else None
            )
            index_val = float(ri.rel_index) if ri.rel_index is not None else None

            for br in by_scope.get(_summary_scope_key(ri), []):
                composite = f"{br.id}_{product_id}_{ri.id}"
                res.append(
                    BatchRunSummaryGQLType(
                        id=hashlib.md5(composite.encode("utf-8")).hexdigest(),
                        run_year=br.run_year,
                        run_month=br.run_month,
                        product_label=product_label,
                        care_type=ri.care_type,
                        calc_date=calc_date,
                        index=index_val,
                    )
                )

        # Keep stable ordering consistent with the old SQL ORDER BY
        res.sort(key=lambda row: (row.run_year or 0, row.run_month or 0))
        return res

    def resolve_relative_indexes(self, info, **kwargs):
        if not info.context.user.has_perms(ClaimBatchConfig.gql_query_relative_indexes_perms):
            raise PermissionDenied(_("unauthorized"))


class Mutation(graphene.ObjectType):
    process_batch = ProcessBatchMutation.Field()
