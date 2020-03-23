import graphene
from django.core.exceptions import PermissionDenied
from django.db import connection
from core import prefix_filterset, ExtendedConnection, filter_validity
from core.schema import TinyInt, SmallInt, OpenIMISMutation, OrderedDjangoFilterConnectionField
from graphene import InputObjectType, ObjectType
from graphene_django import DjangoObjectType
from graphene_django.filter import DjangoFilterConnectionField
from product.schema import ProductGQLType
from location.schema import LocationGQLType
from .models import BatchRun, RelativeIndex
from .services import ProcessBatchSubmit, ProcessBatchService
from .apps import ClaimBatchConfig
from django.utils.translation import gettext as _


class BatchRunGQLType(DjangoObjectType):
    class Meta:
        model = BatchRun
        exclude_fields = ('row_id',)
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "id": ["exact"],
            "run_date": ["exact", "lt", "lte", "gt", "gte"],
            "location": ["isnull"],
            **prefix_filterset("location__", LocationGQLType._meta.filter_fields),
        }
        connection_class = ExtendedConnection


class BatchRunSummaryGQLType(ObjectType):
    run_year = graphene.Int()
    run_month = graphene.Int()
    product_label = graphene.String()
    care_type = graphene.String()
    calc_date = graphene.String()
    index = graphene.Float()

    class Meta:
        interfaces = (graphene.relay.Node,)


def batchRunSummaryFilter(**kwargs):
    filter = ''
    if kwargs.get('accountType'):
        filter += 'r.RelType = %s AND ' % kwargs.get('accountType')
    if kwargs.get('accountYear'):
        filter += 'b.runYear = %s AND ' % kwargs.get('accountYear')
    if kwargs.get('accountMonth'):
        filter += 'b.runMonth = %s AND ' % kwargs.get('accountMonth')
    if kwargs.get('accountDistrict'):
        filter += 'l.LocationId = %s AND ' % kwargs.get('accountDistrict')
    elif kwargs.get('accountRegion'):
        filter += 'l.LocationId = %s AND ' % kwargs.get('accountRegion')
    else:
        filter += 'l.LocationId is NULL AND '
    if kwargs.get('accountProduct'):
        filter += 'r.ProdId = %s AND ' % kwargs.get('accountProduct')
    if kwargs.get('accountCareType'):
        filter += "r.RelCareType = '%s' AND " % kwargs.get('accountCareType')
    return filter + '1 = 1'


class BatchRunSummaryConnection(graphene.Connection):
    class Meta:
        node = BatchRunSummaryGQLType

    total_count = graphene.Int()

    def resolve_total_count(root, info, **kwargs):
        return len(root.iterable)


class RelativeIndexGQLType(DjangoObjectType):
    class Meta:
        model = RelativeIndex
        exclude_fields = ('row_id',)
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
        location_id = graphene.Int(required=False)
        year = graphene.Int()
        month = graphene.Int()

    @classmethod
    def async_mutate(cls, user, **data):
        if not user.has_perms(ClaimBatchConfig.gql_mutation_process_batch_perms):
            raise PermissionDenied(_("unauthorized"))
        submit = ProcessBatchSubmit(
            location_id=data.get('location_id', None),
            year=data['year'],
            month=data['month']
        )
        service = ProcessBatchService(user)
        res = service.submit(submit)
        return res


class Query(graphene.ObjectType):
    batch_runs = OrderedDjangoFilterConnectionField(
        BatchRunGQLType,
        orderBy=graphene.List(of_type=graphene.String))
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

    def resolve_batch_runs(self, info, **kwargs):
        if not info.context.user.has_perms(ClaimBatchConfig.gql_query_batch_runs_perms):
            raise PermissionDenied(_("unauthorized"))

    def resolve_batch_runs_summaries(self, info, **kwargs):
        if not info.context.user.has_perms(ClaimBatchConfig.gql_query_batch_runs_perms):
            raise PermissionDenied(_("unauthorized"))
        sql = '''
        SELECT
            HashBytes('MD5', CONCAT(
                b.RunID, '_',p.ProdId, '_', r.RelIndexID
            )),
            b.RunYear,
            b.RunMonth,
            p.ProductCode,
            p.ProductName,
            r.RelCareType,
            convert(varchar, r.CalcDate, 23),
            r.RelIndex
        FROM
            tblRelIndex r,
            tblLocations l,
            tblBatchRun b,
            tblProduct p
        WHERE
            r.LocationId = l.LocationId AND
            l.LocationId = b.LocationId AND
            r.ProdId = p.ProdId AND %s
        ORDER BY
            b.RunYear,
            b.RunMonth;
        ''' % batchRunSummaryFilter(**kwargs)
        with connection.cursor() as cursor:
            cursor.execute(sql)
            res = [BatchRunSummaryGQLType(
                id=r[0],
                run_year=r[1],
                run_month=r[2],
                product_label=f'{r[3]} {r[4]}',
                care_type=r[5],
                calc_date=r[6],
                index=r[7]
            ) for r in cursor.fetchall()]
            return res

    def resolve_relative_indexes(self, info, **kwargs):
        if not info.context.user.has_perms(ClaimBatchConfig.gql_query_relative_indexes_perms):
            raise PermissionDenied(_("unauthorized"))


class Mutation(graphene.ObjectType):
    process_batch = ProcessBatchMutation.Field()
