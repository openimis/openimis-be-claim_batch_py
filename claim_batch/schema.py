import graphene
from django.db import connection
from django.db.models import Q
from core import prefix_filterset, ExtendedConnection, filter_validity
from core.schema import TinyInt, SmallInt, OpenIMISMutation
from django.conf import settings
from django.contrib.auth.models import AnonymousUser
from django.core.exceptions import ValidationError
from graphene import InputObjectType, ObjectType
from graphene_django import DjangoObjectType
from graphene_django.filter import DjangoFilterConnectionField
from product.schema import ProductGQLType
from location.schema import LocationGQLType
from .models import BatchRun, RelativeIndex
from location.models import Location
from medical.models import Diagnosis


class BatchRunGQLType(DjangoObjectType):
    class Meta:
        model = BatchRun
        exclude_fields = ('row_id',)
        interfaces = (graphene.relay.Node,)
        filter_fields = {
            "id": ["exact"],
            "run_date": ["exact", "lt", "lte", "gt", "gte"],
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


class Query(graphene.ObjectType):
    batch_runs = DjangoFilterConnectionField(BatchRunGQLType)
    batch_runs_summaries = graphene.List(BatchRunSummaryGQLType)
    relative_indexes = DjangoFilterConnectionField(RelativeIndexGQLType)

    def resolve_batch_runs_summaries(self, info, **kwargs):
        sql = '''
        SELECT
            b.RunYear,
            b.RunMonth,
            p.ProductCode,
            p.ProductName,
            r.RelCareType,
            r.CalcDate,
            r.RelIndex
        FROM
            tblRelIndex r,
            tblLocations l,
            tblBatchRun b,
            tblProduct p
        WHERE
            r.LocationId = l.LocationId AND
            l.LocationId = b.LocationId AND
            r.ProdId = p.ProdId;
        '''
        with connection.cursor() as cursor:
            cursor.execute(sql)
            return map(
                lambda r: BatchRunSummaryGQLType(
                    run_year=r[0],
                    run_month=r[1],
                    product_label=f'{r[2]} {r[3]}',
                    care_type=r[4],
                    calc_date=r[5],
                    index=r[6]
                ),
                cursor.fetchall())
