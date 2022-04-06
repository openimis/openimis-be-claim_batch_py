import calendar
import datetime
import uuid
import logging


import core
import pandas as pd
from datetime import date
from claim.models import ClaimItem, Claim, ClaimService, ClaimDetail
from claim_batch.models import BatchRun, RelativeIndex, RelativeDistribution
from django.db import connection, transaction
from django.db.models import Value, F, Sum, Q, Prefetch, Count
from django.db.models.functions import Coalesce, ExtractMonth, ExtractYear
from django.utils.translation import gettext as _
from location.models import HealthFacility, Location
from product.models import Product, ProductItemOrService
from core.signals import *
from contribution.models import Premium
from contribution_plan.models import PaymentPlan
from calculation.services import run_calculation_rules

logger = logging.getLogger(__name__)


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
        if self.batch_run_already_executed(submit.year, submit.month, submit.location_id):
            return str(ProcessBatchSubmitError(2))

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
                except Exception:
                    pass
                finally:
                    next = cur.nextset()
            if res[0] != 0:  # zero means "all done"
                return str([ProcessBatchSubmitError(res[0])])
        self.capitation_report_data_for_summit(submit)

    @classmethod
    def capitation_report_data_for_summit(cls, submit):
        capitation_payment_products = []
        for svc_item in [ClaimItem, ClaimService]:
            capitation_payment_products.extend(
                svc_item.objects
                    .filter(claim__status=Claim.STATUS_VALUATED)
                    .filter(claim__validity_to__isnull=True)
                    .filter(validity_to__isnull=True)
                    .filter(status=svc_item.STATUS_PASSED)
                    .annotate(prod_location=Coalesce("product__location_id", Value(-1)))
                    .filter(prod_location=submit.location_id if submit.location_id else -1)
                    .values('product_id')
                    .distinct()
            )

        region_id, district_id = _get_capitation_region_and_district(submit.location_id)
        for product in set(map(lambda x: x['product_id'], capitation_payment_products)):
            params = {
                'region_id': region_id,
                'district_id': district_id,
                'prod_id': product,
                'year': submit.year,
                'month': submit.month,
            }
            is_report_data_available = get_commision_payment_report_data(params)
            if not is_report_data_available:
                process_capitation_payment_data(params)
            else:
                logger.debug(F"Capitation payment data for {params} already exists")

    @classmethod
    def batch_run_already_executed(cls, year, month, location_id):
        return BatchRun.objects \
            .filter(run_year=year) \
            .filter(run_month=month) \
            .annotate(nn_location_id=Coalesce("location_id", Value(-1))) \
            .filter(nn_location_id=-1 if location_id is None else location_id) \
            .filter(validity_to__isnull=True)\
            .exists()

def create_relative_index(prod_id, prod_value, year, relative_type, location_id, audit_user_id, rel_price_type,
                          period=None, month_start=None, month_end=None):
    logger.debug("Creating relative index for product %s with value %s on year %s, type %s, location %s, "
                 "rel_price_type %s, period %s, month range %s-%s", prod_id, prod_value, year, relative_type,
                 location_id, rel_price_type, period, month_start, month_end)
    distr = RelativeDistribution.objects \
        .filter(product_id=prod_id) \
        .filter(period=period) \
        .filter(type=relative_type) \
        .filter(care_type=rel_price_type) \
        .filter(validity_to__isnull=False) \
        .first()
    distr_perc = distr.percent if distr and distr.percent else 1

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
            qs_val = qs_val.filter(nn_process_stamp_month__gte=month_start).filter(
                nn_process_stamp_month__lte=month_end)
        # else not needed as the year simply relies on the above year filter

        if rel_price_type == RelativeIndex.CARE_TYPE_IN_PATIENT:
            qs_val = qs_val.filter(claim__health_facility__level=HealthFacility.LEVEL_HOSPITAL)
        elif rel_price_type == RelativeIndex.CARE_TYPE_OUT_PATIENT:
            qs_val = qs_val.exclude(claim__health_facility__level=HealthFacility.LEVEL_HOSPITAL)
        # else both, no filter needed

        price_valuated = qs_val.values("price_valuated").aggregate(sum=Sum(Coalesce("price_valuated", 0)))["sum"]
        claim_value += price_valuated if price_valuated else 0

    if claim_value == 0:
        rel_index = 1
    else:
        rel_index = (prod_value * distr_perc) / claim_value

    from core.utils import TimeUtils
    return RelativeIndex.objects.create(
        product_id=prod_id,
        type=relative_type,
        care_type=rel_price_type,
        year=year,
        period=period,
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

    # Transactional stuff
    already_run_batch = BatchRun.objects \
        .filter(run_year=year) \
        .filter(run_month=period) \
        .annotate(nn_location_id=Coalesce("location_id", Value(-1))) \
        .filter(nn_location_id=-1 if location_id is None else location_id) \
        .filter(validity_to__isnull=True).values("id").first()

    if already_run_batch:
        return [str(ProcessBatchSubmitError(2))]
    _, days_in_month = calendar.monthrange(year, period)
    end_date = datetime.datetime(year, period, days_in_month)
    now = datetime.datetime.now()
    # TODO - double check this condition
    #if end_date < now:
    #    return [str(ProcessBatchSubmitError(3))]
        ## TODO create message "Batch cannot be run before the end of the selected period"
    try:
        do_process_batch(audit_user_id, location_id, end_date)
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as exc:
        logger.warning(
            f"Exception while processing batch user {audit_user_id}, location {location_id}, period {period}, year {year}",
            exc_info=True
        )
        return [str(ProcessBatchSubmitError(-1, str(exc)))]


def _get_capitation_region_and_district(location_id):
    if not location_id:
        return None, None
    location = Location.objects.get(id=location_id)
    region_id = None
    district_id = None

    if location.type == 'D':
        district_id = location_id
        region_id = location.parent.id
    elif location.type == 'R':
        region_id = location.id

    return region_id, district_id


def do_process_batch(audit_user_id, location_id, end_date):
    processed_ids = set()  # As we update claims, we add the claims not in relative pricing and then update the status
    period = end_date.month
    year = end_date.year
    logger.debug("do_process_batch location %s for %s/%s", location_id, period, year)

    from core.utils import TimeUtils
    created_run = BatchRun.objects.create(location_id=location_id, run_year=year, run_month=period,
                                          run_date=TimeUtils.now(), audit_user_id=audit_user_id,
                                          validity_from=TimeUtils.now())
    logger.debug("do_process_batch created run: %s", created_run.id)

    # 0 prepare the batch run :  does it really make sense
    # per location ? (Ideally per pool but the notion doesn't exist yet)
    # 0.1 get all product concerned, all product that have are configured for the location
    # init start dates
    start_date = None

    # period_quarter = period - 2 if period % 3 == 0 else 0
    # period_sem = period - 5 if period % 6 == 0 else 0

    products = get_product_queryset(end_date, location_id)
    # 1 per product (Ideally per pool but the notion doesn't exist yet)
    if products:
        for product in products:

            logger.debug("do_process_batch creating work_data for batch run process")
            work_data = {}
            work_data["created_run"] = created_run
            work_data["product"] = product
            work_data["end_date"] = end_date
            logger.debug("do_process_batch created work_data for batch run process")
            allocated_contribution = []
            # 1.2 get all the payment plan per product
            work_data["payment_plans"] = get_payment_plan_queryset(product, end_date)
            if work_data["payment_plans"]:
                for payment_plan in work_data["payment_plans"]:
                    start_date = get_start_date(end_date, payment_plan.periodicity)            
                    start_date_str = str(start_date)                  
                    if start_date_str not in allocated_contribution:  
                        allocated_contribution[start_date_str] = get_allocated_premium(work_data["contributions"], start_date, end_date)
                    work_data['allocated_contributions'] = allocated_contribution[start_date_str]
                    work_data = update_work_data(work_data, product, start_date, end_date, allocated_contribution[str(start_date)])
                    # valuate the claims
                    rcr = run_calculation_rules(payment_plan, "BatchValuate", None,
                                                work_data=work_data, audit_user_id=audit_user_id,
                                                location_id=location_id, start_date=start_date, end_date=end_date)
                    if rcr:
                        logger.debug("valuation processed for: %s", rcr[0][0])

            # 5 Generate BatchPayment per product (Ideally per pool but the notion doesn't exist yet)
            # 5.1 filter a calculation valid for batchRun with context BatchPayment (got via 0.2)
            if work_data["payment_plans"]:
                for payment_plan in work_data["payment_plans"]:
                    start_date = get_start_date(end_date, payment_plan.periodicity)          
                    work_data = update_work_data(work_data, product, start_date, end_date, allocated_contribution[str(start_date)])
                    # 54.2 Execute the converter per product/batch run/claim (not claims)
                    rcr = run_calculation_rules(payment_plan, "BatchPayment", None,
                                                work_data=work_data, audit_user_id=audit_user_id,
                                                location_id=location_id, start_date=start_date, end_date=end_date)
                    if rcr:
                        logger.debug("conversion processed for: %s", rcr[0][0])

            # save the batch run into db
            logger.debug("do_process_batch created run: %s", created_run.id)
    else:
        logger.info("no product found in  %s for %s/%s", location_id, period, year)


def update_work_data(work_data, product, start_date, end_date, allocated_contribution):
    work_data["start_date"] = start_date
    # 1.3 generate queryset
    work_data["items"] = get_items_queryset(product, start_date, end_date)
    work_data["services"] = get_services_queryset(product, start_date, end_date)
    work_data["contributions"] = get_contribution_queryset(product, start_date, end_date)
    work_data['claims'] =  get_claim_queryset(product, start_date, end_date)
    work_data['allocated_contributions'] = allocated_contribution
    return work_data


def get_payment_plan_queryset(product, end_date):
    return PaymentPlan.objects\
        .filter(date_valid_to__gte=end_date)\
        .filter(date_valid_from__lte=end_date)\
        .filter(benefit_plan=product)\
        .filter(is_deleted=False)



def get_items_queryset(product, start_date, end_date):
    return ClaimItem.objects\
        .filter(validity_to__isnull=True)\
        .filter(claim__process_stamp__lte=end_date)\
        .filter(claim__process_stamp__gte=start_date)\
        .filter(product=product)\
        .select_related('claim__health_facility')\
        .order_by('claim__health_facility').order_by('claim_id')

def get_services_queryset(product, start_date, end_date):
    return ClaimService.objects\
        .filter(claim__process_stamp__lte=end_date)\
        .filter(claim__process_stamp__gte=start_date)\
        .filter(validity_to__isnull=True)\
        .filter(product=product)\
        .select_related('claim__health_facility')\
        .order_by('claim__health_facility').order_by('claim_id')
    
def get_claim_queryset(product, start_date, end_date):
    return  Claim.objects\
        .filter(validity_from__lte=end_date)\
        .filter(validity_from__gte=start_date)\
        .filter(validity_to__isnull=True)\
        .filter(process_stamp__lte=end_date)\
        .filter((Q(items__product=product) | Q(services__product=product)))
    
def get_contribution_queryset(product, start_date, end_date):
    return Premium.objects \
        .filter(validity_from__lte=end_date) \
        .filter(validity_from__gte=start_date) \
        .filter(validity_to__isnull=True)\
        .filter(policy__product=product)\
        .select_related('policy')

def get_product_queryset(end_date, location_id):
    return Product.objects\
        .filter(validity_to__isnull = True)\
        .filter(date_from__lte=end_date)\
        .filter(Q(date_to__gte=end_date) | Q(date_to__isnull=True))\
        .filter(location_id=location_id)

# Calculate allcated contributions
def get_allocated_premium(premiums, start_date, end_date):
    # go trough the contribution and find the allocated contribution 
    allocated_premiums = 0
    for premium in premiums:
        allocation_start = max(premium.policy.effective_date, start_date)
        allocation_stop = min(start_date, premium.policy.expiry_date)
        allocation_diff = (allocation_stop - allocation_start).days + 1
        policy_duration = (premium.policy.expiry_date - premium.policy.effective_date).days + 1
        allocated_premiums += premium.amount * allocation_diff / policy_duration
    return allocated_premiums


def get_period( start_date, end_date):
    # TODO do function that returns such values M/Q/Y , 1-12/1-4/1
    period_type = None
    period_id = None
    if start_date.month == end_date.month:
        period_type = '12'
        period_id = end_date.month
    elif start_date.month % 3 == 1 and end_date.month % 3 == 0:
        period_type = '4'
        period_id = end_date.month / 3
    elif start_date.month % 6 == 1 and end_date.month % 6 == 0:
        period_type = '2'
        period_id = end_date.month / 6
    elif start_date.month == 1 and end_date.month == 12:
        period_type = '1'
        period_id = '12'

    return period_type, period_id


def get_start_date(end_date, periodicity):
    # create the possible start dates 
    year = end_date.year
    month = end_date.month
    if periodicity == '12':
        #yearly
        return datetime.date(year, 1, 1) if month == 12 else None
    elif periodicity == '3':
        # quarter
        return datetime.date(year, month - 2, 1) if month % 3 == 0 else None    
    elif periodicity == '6':
        #semester
        return datetime.date(year, month - 5, 1) if month % 6 == 0 else None
    elif periodicity == '1':
        #monthy
        return datetime.date(year, month, 1)
    else:
        return None



def _get_relative_index(product_id, relative_period, relative_year, relative_care_type='B', relative_type=12):
    qs = RelativeIndex.objects \
        .filter(product_id=product_id) \
        .filter(care_type=relative_care_type) \
        .filter(type=relative_type) \
        .filter(year=relative_year) \
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
            except Exception:
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
            except Exception:
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


def process_capitation_payment_data(params):
    with connection.cursor() as cur:
        # HFLevel based on
        # https://github.com/openimis/web_app_vb/blob/2492c20d8959e39775a2dd4013d2fda8feffd01c/IMIS_BL/HealthFacilityBL.vb#L77
        _execute_capitation_payment_procedure(cur, 'uspCreateCapitationPaymentReportData', params)


def get_commision_payment_report_data(params):
    with connection.cursor() as cur:
        # HFLevel based on
        # https://github.com/openimis/web_app_vb/blob/2492c20d8959e39775a2dd4013d2fda8feffd01c/IMIS_BL/HealthFacilityBL.vb#L77
        _execute_capitation_payment_procedure(cur, 'uspSSRSRetrieveCapitationPaymentReportData', params)

        # stored proc outputs several results,
        # we are only interested in the last one
        next = True
        data = None
        while next:
            try:
                data = cur.fetchall()
            except Exception as e:
                pass
            finally:
                next = cur.nextset()
    return data


def _execute_capitation_payment_procedure(cursor, procedure, params):
    sql = F"""\
                DECLARE @HF AS xAttributeV;

                INSERT INTO @HF (Code, Name) VALUES ('D', 'Dispensary');
                INSERT INTO @HF (Code, Name) VALUES ('C', 'Health Centre');
                INSERT INTO @HF (Code, Name) VALUES ('H', 'Hospital');

                EXEC [dbo].[{procedure}]
                    @RegionId = %s,
                    @DistrictId = %s,
                    @ProdId = %s,
                    @Year = %s,
                    @Month = %s,	
                    @HFLevel = @HF;
            """

    cursor.execute(sql, ( 
        params.get('region_id', None),
        params.get('district_id', None),
        params.get('prod_id', 0),
        params.get('year', 0),
        params.get('month', 0),
    ))


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
                 'SUMHF_PriceAsked': health_facilities_sum['PriceAsked'][
                     (row['RegionName'], row['DistrictName'], row['HFCode'])],
                 'SUMHF_PriceApproved': health_facilities_sum['PriceApproved'][
                     (row['RegionName'], row['DistrictName'], row['HFCode'])],
                 'SUMHF_PriceAdjusted': health_facilities_sum['PriceAdjusted'][
                     (row['RegionName'], row['DistrictName'], row['HFCode'])],
                 'SUMHF_RemuneratedAmount': health_facilities_sum['RemuneratedAmount'][
                     (row['RegionName'], row['DistrictName'], row['HFCode'])]
                 } for row in data]
    else:
        data = [{**row,
                 **region_and_district_sums(row, regions_sum, districts_sum, show_claims),
                 'SUMHF_RemuneratedAmount': health_facilities_sum[
                     (row['RegionName'], row['DistrictName'], row['HFCode'])]
                 } for row in data]
    return sorted(data, key=lambda i: (
        i['RegionName'], i['DistrictName'], i['HFCode']))


def add_sums_by_prod(data, regions_sum, districts_sum, products_sum, show_claims):
    if show_claims:
        data = [{**row,
                 **region_and_district_sums(row, regions_sum, districts_sum, show_claims),
                 'SUMP_PriceAsked': products_sum['PriceAsked'][
                     (row['RegionName'], row['DistrictName'], row['ProductCode'])],
                 'SUMP_PriceApproved': products_sum['PriceApproved'][
                     (row['RegionName'], row['DistrictName'], row['ProductCode'])],
                 'SUMP_PriceAdjusted': products_sum['PriceAdjusted'][
                     (row['RegionName'], row['DistrictName'], row['ProductCode'])],
                 'SUMP_RemuneratedAmount': products_sum['RemuneratedAmount'][
                     (row['RegionName'], row['DistrictName'], row['ProductCode'])]
                 } for row in data]
    else:
        data = [{**row,
                 **region_and_district_sums(row, regions_sum, districts_sum, show_claims),
                 'SUMP_RemuneratedAmount': products_sum[(row['RegionName'], row['DistrictName'], row['ProductCode'])]
                 } for row in data]
    return sorted(data, key=lambda i: (
        i['RegionName'], i['DistrictName'], i['ProductCode']))


class ReportDataService(object):
    def __init__(self, user):
        self.user = user

    def fetch(self, prms):
        show_claims = prms.get("showClaims", "false") == "true"
        group = prms.get("group", "H")

        if show_claims:
            data = process_batch_report_data_with_claims(prms)
        else:
            data = process_batch_report_data(prms)
        if not data:
            raise ValueError(_("claim_batch.reports.nodata"))
        df = pd.DataFrame.from_dict(data)
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
