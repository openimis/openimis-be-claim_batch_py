import calendar
import datetime
import uuid

import core
import pandas as pd
from claim.models import ClaimItem, Claim, ClaimService, ClaimDetail
from claim_batch.models import BatchRun, RelativeIndex, RelativeDistribution
from django.db import connection, transaction
from django.db.models import Value, F, Sum
from django.db.models.functions import Coalesce, ExtractMonth, ExtractYear
from django.utils.translation import gettext as _
from location.models import HealthFacility
from product.models import ProductItem, Product, ProductService, ProductItemOrService


@core.comparable
class ProcessBatchSubmit(object):
    def __init__(self, location_id, year, month):
        self.location_id = location_id
        self.year = year
        self.month = month


@core.comparable
class ProcessBatchSubmitError(Exception):
    ERROR_CODES = {
        1: "General fault",
        2: "Already run before",
    }

    def __init__(self, code, msg=None):
        self.code = code
        self.msg = ProcessBatchSubmitError.ERROR_CODES.get(
            self.code, msg or "Unknown exception")

    def __str__(self):
        return "ProcessBatchSubmitError %s: %s" % (self.code, self.msg)


class ProcessBatchService(object):

    def __init__(self, user):
        self.user = user

    def submit(self, submit):
        return process_batch(self.user.i_user.id, submit.location_id, submit.month, submit.year)

    def old_submit(self, submit):
        with connection.cursor() as cur:
            sql = """\
                DECLARE @ret int;
                EXEC @ret = [dbo].[uspBatchProcess] @AuditUser = %s, @LocationId = %s, @Year = %s, @Period = %s;
                SELECT @ret;
            """
            cur.execute(sql, (self.user.i_user.id, submit.location_id,
                              submit.year, submit.month))
            # stored proc outputs several results,
            # we are only interested in the last one
            next = True
            res = None
            while next:
                try:
                    res = cur.fetchone()
                except:
                    pass
                finally:
                    next = cur.nextset()
            if res[0]:  # zero means "all done"
                raise ProcessBatchSubmitError(res[0])


@transaction.atomic
def relative_index_calculation_monthly(rel_type, period, year, location_id, product_id, audit_user_id):
    # TODO (from stored proc) !!!! Check first if not existing in the meantime !!!!!!!

    if rel_type == RelativeIndex.TYPE_MONTH:
        month_start = period
        month_end = period
    elif rel_type == RelativeIndex.TYPE_QUARTER:
        # There is a similar bit of code in calling function but still different, just copying for now
        month_start = period * 3 - 2
        month_end = period * 3
    elif rel_type == RelativeIndex.TYPE_YEAR:
        month_start = 1
        month_end = 12
    else:
        raise Exception("relative type should be month(12), quarter(4) or year(1)")

    for month in range(month_start, month_end + 1):
        with transaction.atomic():
            date = datetime.date(year, period, 1)
            # We don't import the localized calendar because this process is currently based on gregorian calendar
            _, days_in_month = calendar.monthrange(year, period)
            end_date = datetime.date(year, period, days_in_month)

            # For some reason the temp table is not always deleted when we arrive here, so we generate a random name
            table_name = "#Numerator" + uuid.uuid4().hex
            cursor = connection.cursor()
            cursor.execute(f"""
            CREATE TABLE {table_name}
            (
                LocationId int,
                ProdID     int,
                Value      decimal(18, 2),
                WorkValue  int
            )
            """)

            #insert into numerator (location_id, product_id, value, work_value)
            # The Django approach to that query doesn't work in Django 2 as it needs a boolean condition on the F in When
            # Policy.objects\
            #     .filter(validity_to__isnull=True)\
            #     .filter(premium__validity_to__isnull=True)\
            #     .filter(product__validity_to__isnull=True)\
            #     .annotate(nn_product_location_id=Coalesce("product__location_id", Value(-1)))\
            #     .filter(nn_product_location_id=location_id if location_id else -1)\
            #     .filter(Q(product_id=product_id) | Q(product_id=0))\
            #     .exclude(status=Policy.STATUS_IDLE)\
            #     .filter(premium__pay_date__lt=F("expiry_date"))\
            #     .annotate(allocated=
            #               Case(
            #                   When(
            #                       ExtractMonth(F("policy__expiry_date")-1)==month,
            #                       then=Value(1)
            #                   )
            #               )
            #     )

            # Note that pyodbc doesn't support named parameters, so Django will turn all parameters into ? which won't
            # match the parameters anymore (order and multiple times the same name) => not dict, just a list
            # also isnull(%s, -1) fails to bind when the value is... null => composition the condition
            params = [
                month, year, date, date, date, date, month, year, days_in_month, date, end_date, date, days_in_month,
            ]
            if location_id:
                sql_location_condition = " AND Prod.LocationId=%s "
                params.append(location_id)
            else:
                sql_location_condition = " AND Prod.LocationId is null "
            params += [product_id, product_id]

            sql = f"""
                INSERT INTO {table_name} (LocationId, ProdID, Value, WorkValue)
                --Get all the payment falls under the current month and assign it to Allocated
                SELECT NumValue.LocationId, NumValue.ProdID, ISNULL(SUM(NumValue.Allocated), 0) Allocated, 1
                FROM (
                         SELECT L.LocationId, Prod.ProdID,
                                CASE
                                    WHEN MONTH(DATEADD(D, -1, PL.ExpiryDate)) = %s AND
                                         YEAR(DATEADD(D, -1, PL.ExpiryDate)) = %s AND (DAY(PL.ExpiryDate)) > 1
                                        THEN CASE
                                                 WHEN DATEDIFF(D,
                                                     CASE WHEN PR.PayDate < %s THEN %s ELSE PR.PayDate END,
                                                        PL.ExpiryDate) = 0 THEN 1
                                                 ELSE DATEDIFF(D,
                                                     CASE
                                                         WHEN PR.PayDate < %s THEN %s
                                                         ELSE PR.PayDate END, PL.ExpiryDate)
                                             END
                                             * ((SUM(PR.Amount)) / (
                                                 CASE
                                                     WHEN (DATEDIFF(DAY,
                                                         CASE
                                                             WHEN PR.PayDate < PL.EffectiveDate
                                                                 THEN PL.EffectiveDate
                                                             ELSE PR.PayDate
                                                         END,
                                                         PL.ExpiryDate)) <= 0 THEN 1
                                                     ELSE DATEDIFF(
                                                             DAY,
                                                             CASE
                                                                 WHEN PR.PayDate < PL.EffectiveDate
                                                                     THEN PL.EffectiveDate
                                                                 ELSE PR.PayDate END,
                                                             PL.ExpiryDate) END))
                                    WHEN MONTH(CASE
                                                   WHEN PR.PayDate < PL.EffectiveDate THEN PL.EffectiveDate
                                                   ELSE PR.PayDate
                                               END) = %s
                                         AND YEAR(CASE
                                                  WHEN PR.PayDate < PL.EffectiveDate
                                                      THEN PL.EffectiveDate
                                                  ELSE PR.PayDate
                                                  END) = %s
                                        THEN ((%s + 1 - DAY(CASE
                                                                          WHEN PR.PayDate < PL.EffectiveDate
                                                                              THEN PL.EffectiveDate
                                                                          ELSE PR.PayDate END)) *
                                              ((SUM(PR.Amount)) /
                                               CASE
                                                  WHEN DATEDIFF(DAY, CASE
                                                                         WHEN PR.PayDate < PL.EffectiveDate
                                                                             THEN PL.EffectiveDate
                                                                         ELSE PR.PayDate
                                                                     END,
                                                                PL.ExpiryDate) <= 0 THEN 1
                                                  ELSE DATEDIFF(DAY, CASE
                                                                         WHEN PR.PayDate < PL.EffectiveDate
                                                                             THEN PL.EffectiveDate
                                                                         ELSE PR.PayDate END,
                                                                PL.ExpiryDate)
                                               END))
                                    WHEN PL.EffectiveDate < %s AND PL.ExpiryDate > %s AND PR.PayDate < %s
                                        THEN %s * (SUM(PR.Amount) /
                                            CASE
                                              WHEN (DATEDIFF(DAY, CASE
                                                                      WHEN PR.PayDate < PL.EffectiveDate
                                                                          THEN PL.EffectiveDate
                                                                      ELSE PR.PayDate END,
                                                             DATEADD(D, -1, PL.ExpiryDate))) <=
                                                   0 THEN 1
                                              ELSE DATEDIFF(DAY, CASE
                                                                     WHEN PR.PayDate < PL.EffectiveDate
                                                                         THEN PL.EffectiveDate
                                                                     ELSE PR.PayDate END,
                                                            PL.ExpiryDate) END)
                                    END Allocated
                         FROM tblPremium PR
                                  INNER JOIN tblPolicy PL ON PR.PolicyID = PL.PolicyID
                                  INNER JOIN tblProduct Prod ON PL.ProdID = Prod.ProdID
                                  LEFT JOIN tblLocations L ON ISNULL(Prod.LocationId, -1) = ISNULL(L.LocationId, -1)
                         WHERE PR.ValidityTo IS NULL
                           AND PL.ValidityTo IS NULL
                           AND Prod.ValidityTo IS NULL
                           {sql_location_condition}
                           AND (Prod.ProdID=%s or %s=0)
                           AND PL.PolicyStatus <> 1
                           AND PR.PayDate <= PL.ExpiryDate
    
                         GROUP BY L.LocationId, Prod.ProdID, PR.Amount, PR.PayDate, PL.ExpiryDate, PL.EffectiveDate
                     ) NumValue
                GROUP BY LocationId, ProdID
            """
            cursor.execute(sql, params)

            cursor.execute(f"""
                INSERT INTO {table_name} (LocationId, ProdID, Value, WorkValue)
                SELECT LocationId, ProdID, ISNULL(SUM(Value), 0) Allocated, 0
                FROM {table_name}
                GROUP BY LocationId, ProdID
            """)
            cursor.execute(f"""
                DELETE FROM {table_name} WHERE WorkValue = 1
            """)

            cursor.execute(f"SELECT ProdID, Value FROM {table_name}")
            rel_price_mapping = [
                ("period_rel_prices", "B"),
                ("period_rel_prices_ip", "I"),
                ("period_rel_prices_op", "O")
            ]
            for prod_id, prod_value in cursor.fetchall():
                product = Product.objects.filter(id=prod_id).first()
                if rel_type == RelativeIndex.TYPE_MONTH:
                    for rel_price_item, rel_price_type in rel_price_mapping:
                        if product and getattr(product, rel_price_item) == Product.RELATIVE_PRICE_PERIOD_MONTH:
                            create_relative_index(prod_id, prod_value, year, rel_type, location_id, audit_user_id,
                                                  rel_price_type, period=period)

                if rel_type == RelativeIndex.TYPE_QUARTER:
                    for rel_price_item, rel_price_type in rel_price_mapping:
                        if product and getattr(product, rel_price_item) == Product.RELATIVE_PRICE_PERIOD_QUARTER:
                            create_relative_index(prod_id, prod_value, year, rel_type, location_id, audit_user_id,
                                                  rel_price_type, month_start=month_start, month_end=month_end)

                if rel_type == RelativeIndex.TYPE_YEAR:
                    for rel_price_item, rel_price_type in rel_price_mapping:
                        if product and getattr(product, rel_price_item) == Product.RELATIVE_PRICE_PERIOD_YEAR:
                            create_relative_index(prod_id, prod_value, year, rel_type, location_id, audit_user_id,
                                              rel_price_type)


def create_relative_index(prod_id, prod_value, year, relative_type, location_id, audit_user_id, rel_price_type,
                          period=None, month_start=None, month_end=None):
    distr = RelativeDistribution.objects \
        .filter(product_id=prod_id) \
        .filter(period=period) \
        .filter(type=relative_type) \
        .filter(care_type=rel_price_type) \
        .filter(validity_to__isnull=False) \
        .first()
    distr_perc = distr.percent if distr and distr.percent else 1.0

    claim_value = 0
    for claim_detail in [ClaimService, ClaimItem]:
        qs_val = claim_detail.objects \
            .filter(status=ClaimDetail.STATUS_PASSED) \
            .filter(claim__validity_to__isnull=True) \
            .filter(validity_to__isnull=True) \
            .filter(claim__status__in=[Claim.STATUS_PROCESSED, Claim.STATUS_VALUATED]) \
            .annotate(nn_process_stamp_month=Coalesce(ExtractMonth("claim__process_stamp"), Value(-1))) \
            .annotate(nn_process_stamp_year=Coalesce(ExtractYear("claim__process_stamp"), Value(-1))) \
            .filter(nn_process_stamp_year=year) \
            .filter(product_id=prod_id)
        if period:
            qs_val = qs_val.filter(nn_process_stamp_month=period)
        elif month_start and month_end:
            qs_val = qs_val.filter(nn_process_stamp_month__gte=month_start).filter(nn_process_stamp_month__lte=month_end)
        # else not needed as the year simply relies on the above year filter

        if rel_price_type == RelativeIndex.CARE_TYPE_IN_PATIENT:
            qs_val = qs_val.filter(claim__health_facility__level=HealthFacility.LEVEL_HOSPITAL)
        elif rel_price_type == RelativeIndex.CARE_TYPE_OUT_PATIENT:
            qs_val = qs_val.exclude(claim__health_facility__level=HealthFacility.LEVEL_HOSPITAL)
        # else both, no filter needed

        price_valuated = qs_val.aggregate(Sum(Coalesce("price_valuated", 0)))
        claim_value += price_valuated["distribution_percent__sum"] if price_valuated else 0

    if claim_value == 0:
        rel_index = 1
    else:
        rel_index = (prod_value * distr_perc) / claim_value

    from core.utils import TimeUtils
    return RelativeIndex.objects.create(
        product_id=prod_id,
        relative_type=relative_type,
        care_type=RelativeIndex.CARE_TYPE_IN_PATIENT,
        year=year,
        month=period,
        calc_date=TimeUtils.now(),
        rel_index=rel_index,
        audit_user_id=audit_user_id,
        location_id=location_id,
    )


@transaction.atomic
def process_batch(audit_user_id, location_id, period, year):
    # declare table tblClaimsIDs
    if location_id == -1:
        location_id = None

    result = 0  # OK

    # Transactional stuff
    already_run_batch = BatchRun.objects\
        .filter(run_year=year)\
        .filter(run_month=period)\
        .annotate(nn_location_id=Coalesce("location_id", Value(-1)))\
        .filter(nn_location_id=-1 if location_id is None else location_id)\
        .filter(validity_to__isnull=False).values("id").first()

    if already_run_batch:
        # Already run
        return 2

    relative_index_calculation_monthly(rel_type=12, period=period, year=year, location_id=location_id, product_id=0,
                                       audit_user_id=audit_user_id)
    if period == 3:
        relative_index_calculation_monthly(rel_type=4, period=1, year=year, location_id=location_id, product_id=0,
                                           audit_user_id=audit_user_id)
    if period == 6:
        relative_index_calculation_monthly(rel_type=4, period=2, year=year, location_id=location_id, product_id=0,
                                           audit_user_id=audit_user_id)
    if period == 9:
        relative_index_calculation_monthly(rel_type=4, period=3, year=year, location_id=location_id, product_id=0,
                                           audit_user_id=audit_user_id)
    if period == 12:
        relative_index_calculation_monthly(rel_type=4, period=4, year=year, location_id=location_id, product_id=0,
                                           audit_user_id=audit_user_id)
        relative_index_calculation_monthly(rel_type=1, period=1, year=year, location_id=location_id, product_id=0,
                                           audit_user_id=audit_user_id)

    for svc_item in [ClaimItem, ClaimService]:
        prod_qs = svc_item.objects\
            .filter(claim__status=Claim.STATUS_PROCESSED)\
            .filter(claim__validity_to__isnull=True)\
            .filter(validity_to__isnull=True)\
            .filter(status=svc_item.STATUS_PASSED)\
            .filter(price_origin=ProductItemOrService.ORIGIN_RELATIVE)\
            .annotate(prod_location=Coalesce("product__location_id", Value(-1)))\
            .filter(prod_location=location_id if location_id else -1)

        product_loop = prod_qs.values(
            "claim__health_facility__level", "product_id", "product__period_rel_prices", "product__period_rel_prices_op",
            "product__period_rel_prices_ip", "claim__process_stamp__month", "claim__process_stamp__year") \
            .distinct()

        for product in product_loop:
            index = -1
            target_month = product["claim__process_stamp__month"]
            target_year = product["claim__process_stamp__year"]
            # Will fail with Ethiopian calendar but so will the rest of this procedure
            target_quarter = int((target_month-1)/3)+1

            index = -1
            if product["product__period_rel_prices"]:
                prod_rel_price_type = int(product["product__period_rel_prices"])
            elif product.level == 'H' and product["product__period_rel_prices_ip"]:
                prod_rel_price_type = int(product["product__period_rel_prices_ip"])
            elif product.level != 'H' and product["product__period_rel_prices_op"]:
                prod_rel_price_type = int(product["product__period_rel_prices_op"])
            else:
                # This should normally be impossible but the linter is not so sure
                raise Exception(f"product {product['product_id']} has an impossible in/out patient or both")

            if prod_rel_price_type == RelativeIndex.TYPE_MONTH:
                index = _get_relative_index(product["product_id"], target_month, target_year, RelativeIndex.CARE_TYPE_BOTH,
                                            RelativeIndex.TYPE_MONTH)
            if prod_rel_price_type == RelativeIndex.TYPE_QUARTER:
                index = _get_relative_index(product["product_id"], target_quarter, target_year,
                                            RelativeIndex.CARE_TYPE_BOTH, RelativeIndex.TYPE_QUARTER)
            if prod_rel_price_type == RelativeIndex.TYPE_YEAR:
                index = _get_relative_index(product["product_id"], None, target_year, RelativeIndex.CARE_TYPE_BOTH,
                                            RelativeIndex.TYPE_YEAR)

            if index > -1:
                prod_qs \
                    .filter(claim__health_facility__level=product.level)\
                    .filter(product_id=product.product_id)\
                    .update(remunerated_amount=F(F("price_valuated")*index))

    # Get all the claims in valuated state with no Relative index /Services
    def filter_valuated_claims(base):
        return base.objects.filter(claim__status=Claim.STATUS_VALUATED)\
            .filter(claim__validity_to__isnull=True)\
            .filter(validity_to__isnull=True)\
            .filter(status=ClaimDetail.STATUS_PASSED)\
            .exclude(price_origin='R')\
            .annotate(prod_location=Coalesce("product__location_id", Value(-1)))\
            .filter(prod_location=location_id if location_id else -1)\
            .filter(claim__batch_run_id__isnull=True)\
            .filter(claim__process_stamp__month=period)\
            .filter(claim__process_stamp__year=year)\
            .values("claim_id")\
            .distinct()

    item_ids = filter_valuated_claims(ClaimItem)
    service_ids = filter_valuated_claims(ClaimService)

    all_ids = item_ids.union(service_ids).distinct().values_list("id", flat=True)

    def filter_item_or_service(base):
        return base.objects \
            .filter(claim__validity_to__isnull=True) \
            .annotate(prod_location=Coalesce("product__location_id", Value(-1))) \
            .filter(prod_location=location_id if location_id else -1) \
            .filter(remunerated_amount__isnull=True) \
            .filter(validity_to__isnull=True) \
            .filter(status=ClaimItem.STATUS_PASSED) \
            .filter(claim__status=Claim.STATUS_PROCESSED) \
            .values("claim_id") \
            .distinct()

    item_prod_ids = filter_item_or_service(ClaimItem)
    service_prod_ids = filter_item_or_service(ClaimService)

    Claim.objects\
        .filter(status=Claim.STATUS_PROCESSED)\
        .filter(id__in=all_ids)\
        .filter(validity_to__isnull=True)\
        .exclude(id__in=item_prod_ids)\
        .exclude(id__in=service_prod_ids)\
        .update(status=Claim.STATUS_VALUATED)

    from core.utils import TimeUtils
    created_run = BatchRun.objects.create(location_id=location_id, run_year=year, run_month=period,
                                          run_date=TimeUtils.now(), audit_user_id=audit_user_id,
                                          validity_from=TimeUtils.now())

    month_start = 0
    month_end = 0
    if period in (3, 6, 9):
        month_start = period - 2
        month_end = period
    if period == 12:
        month_start = 1
        month_end = 12

    # Link claims to this batch run
    filter_base = Claim.objects\
        .filter(id__in=all_ids)\
        .filter(status=Claim.STATUS_VALUATED)\
        .filter(batch_run_id__isnull=True)\
        .filter(process_stamp__year=year)

    filter_base\
        .filter(process_stamp__month=period)\
        .update(batch_run=created_run)

    # If more than a month was run
    if month_start > 0:
        filter_base\
            .filter(process_stamp__month__gte=month_start)\
            .filter(process_stamp__month__lte=month_end)\
            .update(batch_run=created_run)

    return result


def _get_relative_index(product_id, relative_period, relative_year, relative_care_type='B', relative_type=12):
    qs = RelativeIndex.objects\
        .filter(product_id=product_id)\
        .filter(care_type=relative_care_type)\
        .filter(type=relative_type)\
        .filter(year=relative_year)\
        .filter(validity_to__isnull=True)
    if relative_period:
        qs = qs.filter(period=relative_period)
    rel_index = qs.values_list("rel_index", flat=True).first()
    return rel_index if rel_index else -1


def process_batch_report_data_with_claims(prms):
    with connection.cursor() as cur:
        sql = """\
            EXEC [dbo].[uspSSRSProcessBatchWithClaim]
                @LocationId = %s,
                @ProdID = %s,
                @RunID = %s,
                @HFID = %s,
                @HFLevel = %s,
                @DateFrom = %s,
                @DateTo = %s
        """
        cur.execute(sql, (
            prms.get('locationId', 0),
            prms.get('prodId', 0),
            prms.get('runId', 0),
            prms.get('hfId', 0),
            prms.get('hfLevel', ''),
            prms.get('dateFrom', ''),
            prms.get('dateTo', '')
        ))
        # stored proc outputs several results,
        # we are only interested in the last one
        next = True
        data = None
        while next:
            try:
                data = cur.fetchall()
            except:
                pass
            finally:
                next = cur.nextset()
    return [{
        "ClaimCode": row[0],
        "DateClaimed": row[1].strftime("%Y-%m-%d") if row[1] is not None else None,
        "OtherNamesAdmin": row[2],
        "LastNameAdmin": row[3],
        "DateFrom": row[4].strftime("%Y-%m-%d") if row[4] is not None else None,
        "DateTo": row[5].strftime("%Y-%m-%d") if row[5] is not None else None,
        "CHFID": row[6],
        "OtherNames": row[7],
        "LastName": row[8],
        "HFID": row[9],
        "HFCode": row[10],
        "HFName": row[11],
        "AccCode": row[12],
        "ProdID": row[13],
        "ProductCode": row[14],
        "ProductName": row[15],
        "PriceAsked": row[16],
        "PriceApproved": row[17],
        "PriceAdjusted": row[18],
        "RemuneratedAmount": row[19],
        "DistrictID": row[20],
        "DistrictName": row[21],
        "RegionID": row[22],
        "RegionName": row[23]
    } for row in data]


def process_batch_report_data(prms):
    with connection.cursor() as cur:
        sql = """\
            EXEC [dbo].[uspSSRSProcessBatch]
                @LocationId = %s,
                @ProdID = %s,
                @RunID = %s,
                @HFID = %s,
                @HFLevel = %s,
                @DateFrom = %s,
                @DateTo = %s
        """
        cur.execute(sql, (
            prms.get('locationId', 0),
            prms.get('prodId', 0),
            prms.get('runId', 0),
            prms.get('hfId', 0),
            prms.get('hfLevel', ''),
            prms.get('dateFrom', ''),
            prms.get('dateTo', '')
        ))
        # stored proc outputs several results,
        # we are only interested in the last one
        next = True
        data = None
        while next:
            try:
                data = cur.fetchall()
            except:
                pass
            finally:
                next = cur.nextset()
    return [{
        "RegionName": row[0],
        "DistrictName": row[1],
        "HFCode": row[2],
        "HFName": row[3],
        "ProductCode": row[4],
        "ProductName": row[5],
        "RemuneratedAmount": row[6],
        "AccCodeRemuneration": row[7],
        "AccCode": row[8]
    } for row in data]


def regions_sum(df, show_claims):
    if show_claims:
        return df.groupby(['RegionName'])[
            'PriceAsked', 'PriceApproved', 'PriceAdjusted', 'RemuneratedAmount'].sum().to_dict()
    else:
        return df.groupby(['RegionName'])['RemuneratedAmount'].sum().to_dict()


def districts_sum(df, show_claims):
    if show_claims:
        return df.groupby(['RegionName', 'DistrictName'])[
            'PriceAsked', 'PriceApproved', 'PriceAdjusted', 'RemuneratedAmount'].sum().to_dict()
    else:
        return df.groupby(['RegionName', 'DistrictName'])['RemuneratedAmount'].sum().to_dict()


def health_facilities_sum(df, show_claims):
    if show_claims:
        return df.groupby(['RegionName', 'DistrictName', 'HFCode'])[
            'PriceAsked', 'PriceApproved', 'PriceAdjusted', 'RemuneratedAmount'].sum().to_dict()
    else:
        return df.groupby(['RegionName', 'DistrictName', 'HFCode'])['RemuneratedAmount'].sum().to_dict()


def products_sum(df, show_claims):
    if show_claims:
        return df.groupby(['RegionName', 'DistrictName', 'ProductCode'])[
            'PriceAsked', 'PriceApproved', 'PriceAdjusted', 'RemuneratedAmount'].sum().to_dict()
    else:
        return df.groupby(['RegionName', 'DistrictName', 'ProductCode'])['RemuneratedAmount'].sum().to_dict()


def region_and_district_sums(row, regions_sum, districts_sum, show_claims):
    if show_claims:
        return {
            'SUMR_PriceAsked': regions_sum['PriceAsked'][row['RegionName']],
            'SUMR_PriceApproved': regions_sum['PriceApproved'][row['RegionName']],
            'SUMR_PriceAdjusted': regions_sum['PriceAdjusted'][row['RegionName']],
            'SUMR_RemuneratedAmount': regions_sum['RemuneratedAmount'][row['RegionName']],
            'SUMD_PriceAsked': districts_sum['PriceAsked'][(row['RegionName'], row['DistrictName'])],
            'SUMD_PriceApproved': districts_sum['PriceApproved'][(row['RegionName'], row['DistrictName'])],
            'SUMD_PriceAdjusted': districts_sum['PriceAdjusted'][(row['RegionName'], row['DistrictName'])],
            'SUMD_RemuneratedAmount': districts_sum['RemuneratedAmount'][(row['RegionName'], row['DistrictName'])]
        }
    else:
        return {
            'SUMR_RemuneratedAmount': regions_sum[row['RegionName']],
            'SUMD_RemuneratedAmount': districts_sum[(row['RegionName'], row['DistrictName'])]
        }


def add_sums_by_hf(data, regions_sum, districts_sum, health_facilities_sum, show_claims):
    if show_claims:
        data = [{**row,
                 **region_and_district_sums(row, regions_sum, districts_sum, show_claims),
                 'SUMHF_PriceAsked': health_facilities_sum['PriceAsked'][(row['RegionName'], row['DistrictName'], row['HFCode'])],
                 'SUMHF_PriceApproved': health_facilities_sum['PriceApproved'][(row['RegionName'], row['DistrictName'], row['HFCode'])],
                 'SUMHF_PriceAdjusted': health_facilities_sum['PriceAdjusted'][(row['RegionName'], row['DistrictName'], row['HFCode'])],
                 'SUMHF_RemuneratedAmount': health_facilities_sum['RemuneratedAmount'][(row['RegionName'], row['DistrictName'], row['HFCode'])]
                 } for row in data]
    else:
        data = [{**row,
                 **region_and_district_sums(row, regions_sum, districts_sum, show_claims),
                 'SUMHF_RemuneratedAmount': health_facilities_sum[(row['RegionName'], row['DistrictName'], row['HFCode'])]
                 } for row in data]
    return sorted(data, key=lambda i: (
        i['RegionName'], i['DistrictName'], i['HFCode']))


def add_sums_by_prod(data, regions_sum, districts_sum, products_sum, show_claims):
    if show_claims:
        data=[{**row,
                 **region_and_district_sums(row, regions_sum, districts_sum, show_claims),
                 'SUMP_PriceAsked': products_sum['PriceAsked'][(row['RegionName'], row['DistrictName'], row['ProductCode'])],
                 'SUMP_PriceApproved': products_sum['PriceApproved'][(row['RegionName'], row['DistrictName'], row['ProductCode'])],
                 'SUMP_PriceAdjusted': products_sum['PriceAdjusted'][(row['RegionName'], row['DistrictName'], row['ProductCode'])],
                 'SUMP_RemuneratedAmount': products_sum['RemuneratedAmount'][(row['RegionName'], row['DistrictName'], row['ProductCode'])]
                 } for row in data]
    else:
        data=[{**row,
                 **region_and_district_sums(row, regions_sum, districts_sum, show_claims),
                 'SUMP_RemuneratedAmount': products_sum[(row['RegionName'], row['DistrictName'], row['ProductCode'])]
                 } for row in data]
    return sorted(data, key=lambda i: (
        i['RegionName'], i['DistrictName'], i['ProductCode']))


class ReportDataService(object):
    def __init__(self, user):
        self.user=user

    def fetch(self, prms):
        show_claims=prms.get("showClaims", "false") == "true"
        group=prms.get("group", "H")

        if show_claims:
            data=process_batch_report_data_with_claims(prms)
        else:
            data=process_batch_report_data(prms)
        if not data:
            raise ValueError(_("claim_batch.reports.nodata"))
        df=pd.DataFrame.from_dict(data)
        if group == "H":
            return add_sums_by_hf(data,
                                  regions_sum(df, show_claims),
                                  districts_sum(df, show_claims),
                                  health_facilities_sum(df, show_claims),
                                  show_claims)
        else:
            return add_sums_by_prod(data,
                                    regions_sum(df, show_claims),
                                    districts_sum(df, show_claims),
                                    products_sum(df, show_claims),
                                    show_claims)
