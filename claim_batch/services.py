import calendar
import datetime
import uuid
import logging


import core
import pandas as pd
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
#@register_service_signal('signal_after_claim_batch_module_process_batch_service')
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
    end_date = datetime.date(year, period, days_in_month)
    if end_date < today():
        return [str(ProcessBatchSubmitError(3))]
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
    
    

    # 0 prepare the batch run :  does it really make sense per location ? (Ideally per pool but the notion doesn't exist yet)
    # 0.1 get all product concerned, all product that have are configured for the location
    

    # init start dates
    start_date = None
    start_date_ip = None

    
    #period_quarter = period - 2 if period % 3 == 0 else 0
    #period_sem = period - 5 if period % 6 == 0 else 0

    products = Product.objects\
            .filter(validity_to__isnull = True)\
            .filter(date_from__lte=end_date)\
            .filter(Q(date_to__gte=end_date) | Q(date_to__isnull=True))\
            .filter(location_id=location_id)
    # 1 per product (Ideally per pool but the notion doesn't exist yet)
    if products:
        for product in products:
            relative_price_g = False
            # assign start date accordin to product
            if product.period_rel_prices is not None:
                start_date =  get_start_date(end_date, product.period_rel_prices)
                relative_price_g = True
            else:
                if product.period_rel_prices_op is not None:
                    start_date =  get_start_date(end_date, product.period_rel_prices_op)
                    # if only op is defiene fallback ip on the same periodicity
                    if product.period_rel_prices_ip is  None:
                        start_date_ip = start_date
                if product.period_rel_prices_ip is not None:
                    start_date_ip =  get_start_date(end_date, product.period_rel_prices_ip)
                    # if only ip is defiene fallback op on the same periodicity
                    if product.period_rel_prices_op is  None:
                        start_date = start_date_ip
            if start_date is None and start_date_ip is None:
                # fall back on month
                start_date = get_start_date(end_date, 'M')
                relative_price_g = True


            work_data = {}
            work_data["created_run"] = created_run
            work_data["product"] = product
            work_data["start_date"] = start_date
            work_data["start_date_ip"] = start_date_ip
            work_data["end_date"] = end_date
            work_data["relative_price_g"]  = relative_price_g
            # 1.2 get all the payment plan per product
            work_data["payment_plans"] = PaymentPlan.objects\
                .filter(date_valid_to__gte=start_date)\
                .filter(date_valid_from__lte=end_date)\
                .filter(benefit_plan=product)
            # 1.3 retrive all the Item & service per product
            # to be checked if we need to pull the claim too
            work_data["items"] = ClaimItem.objects\
                .filter(validity_to__isnull=True)\
                .filter(claim__process_stamp__lte=end_date)\
                .filter(claim__process_stamp__gte=start_date)\
                .filter(product=product)\
                .select_related('claim__health_facility')\
                .order_by('claim__health_facility').order_by('claim_id')
            work_data["services"] = ClaimService.objects\
                .filter(claim__process_stamp__lte=end_date)\
                .filter(claim__process_stamp__gte=start_date)\
                .filter(validity_to__isnull=True)\
                .filter(product=product)\
                .select_related('claim__health_facility')\
                .order_by('claim__health_facility').order_by('claim_id')
            # 1.4 retrive all contributions per product // process_stamp should be the creation date
            work_data["contributions"] = Premium.objects \
                .filter(validity_from__lte=end_date) \
                .filter(validity_from__gte=start_date) \
                .filter(validity_to__isnull=True)\
                .filter(policy__product=product)\
                .select_related('policy')

            # 2 batchRun pre-calculation
            # update the service and item valuated amount 

            if start_date is not None:
                allocated_contributions = get_allocated_premium(work_data["contributions"], start_date, end_date)
                work_data['allocated_contributions'] = allocated_contributions
                # prevent calculation of 2 time the same thing
                if start_date == start_date_ip:
                    allocated_contributions_ip = allocated_contributions
                    work_data['allocated_contributions_ip'] = allocated_contributions_ip
            if start_date_ip is not None and start_date != start_date_ip :
                allocated_contributions_ip = get_allocated_premium(work_data["contributions"], start_date_ip, end_date)
                work_data['allocated_contributions_ip'] = allocated_contributions_ip
            
            claims = Claim.objects\
                .filter(validity_from__lte=end_date)\
                .filter(validity_from__gte=start_date)\
                .filter(validity_to__isnull=True)\
                .filter(process_stamp__lte=end_date)\
                .filter((Count(items__product=product)+Count(services__product=product)) > 0)
            work_data['claims'] = claims
            # adds the filter to includ only the claims according the start/stop and cieling definition 
            claims = add_hospital_claim_date_filter(claims, relative_price_g, start_date, start_date_ip, end_date)
            # prefectch Item and services

            

            claim_batch_valuation(work_data)
                
            

            # 2.1 [for the futur if there is any need ]filter a calculation valid for batchRun with context BatchPrepare (got via 0.2): like allocated contribution
            #if work_data.paymentplans:
            #    for paymentplan in work_data.paymentplans:
            #        if paymentplan.calculation.active_for_object(batch_run, 'BatchPrepare'):
            #            # work_data gets updated with relevant data such as work_data.allocated_contribution
            #            paymentplan.calculation.calculate(work_data, 'BatchPrepare'))
            #        # 3 Generate Batchvaluation:
            #        # 3.1 filter a calculation valid for batchRun with context BatchValuation (got via 0.2)
            #        if paymentplan.calculation.active_for_object(batch_run, 'BatchValuate'):
            #        # 3.2 Execute the calculation per Item or service (not claims)
            #            paymentplan.calculation.calculate(work_data, 'BatchValuate'))
            #else:
            #    logger.info("no payment plan product found for product  %s for %s/%s", product_id, period, year)
            # 4 update the claim Total amounts if all Item and services got "valuated"


            # could be duplicates - distinct
            claims = claims.prefetch_related(Prefetch('items', queryset=ClaimItem.objects.filter(legacy_id__isnull=True)))\
                .prefetch_related(Prefetch('services', queryset=ClaimService.objects.filter(legacy_id__isnull=True)))
            claims = list(claims.values_list('id', flat=True).distinct())
            claims = Claim.objects.filter(id__in=claims)

            for claim in claims:
                remunerated_amount = 0
                for service in claim.services.all():
                    remunerated_amount = service.remunerated_amount + remunerated_amount if service.remunerated_amount else remunerated_amount
                for item in claim.items.all():
                    remunerated_amount = item.remunerated_amount + remunerated_amount if item.remunerated_amount else remunerated_amount
                if remunerated_amount > 0:
                    claim.valuated = remunerated_amount
                    claim.save()
                claim.batch_run = work_data["created_run"]
                claim.save()

            # 5 Generate BatchPayment per product (Ideally per pool but the notion doesn't exist yet)
            # 5.1 filter a calculation valid for batchRun with context BatchPayment (got via 0.2)
            if work_data["payment_plans"]:
                for payment_plan in work_data["payment_plans"]:
                    # 54.2 Execute the converter per product/batch run/claim (not claims)
                    rcr = run_calculation_rules(payment_plan, "BatchPayment", None,
                                                work_data=work_data, audit_user_id=audit_user_id,
                                                location_id=location_id, start_date=start_date, end_date=end_date)
                    logger.debug("conversion processed for: %s", rcr[0][0])

            # save the batch run into db
            logger.debug("do_process_batch created run: %s", created_run.id)
    else:
        logger.info("no product found in  %s for %s/%s", location_id, period, year)


# Calculate allcated contributions
def get_allocated_premium(premiums, start_date, end_date):
    # go trough the contribution and find the allocated contribution 
    allocated_premiums = 0
    for premium in premiums:
        allocation_start = max(premium.policy.effective_date, start_date)
        allocation_stop = min(start_date, premium.policy.expiry_date)
        policy_duration = premium.policy.expiry_date - premium.policy.effective_date
        allocated_premiums += premium.amount * (allocation_stop - allocation_start) / policy_duration
    return allocated_premiums


def get_period( start_date, end_date):
    # TODO do function that returns such values M/Q/Y , 1-12/1-4/1
    period_type = None
    period_id = None
    if start_date.month == end_date.month:
        period_type = '12'
        period_id = end_date.month
    elif start_date.month % 3 == 1 and  end_date.month % 3 == 0 :
        period_type = '4'
        period_id = end_date.month / 3
    elif start_date.month % 6 == 1 and  end_date.month % 6 == 0 :
        period_type = '2'
        period_id = end_date.month / 6
    elif start_date.month == 1 and  end_date.month == 12 :
        period_type = '1'
        period_id = '12'

    return period_type, period_id

def get_start_date(end_date, relative_price):
    # create the possible start dates 
    year = end_date.year
    month = end_date.month
    start_date_month = datetime.date(year, month, 1)
    start_date_quarter = datetime.date(year, month -2, 1) if month % 3 == 0 else None
    start_date_year = datetime.date(year, 1, 1) if month == 12 else None
    start_date_sem = datetime.date(year, month - 5 , 1) if month % 6 == 0 else None
    if relative_price == 'Y':
        return start_date_year
    elif relative_price == 'Q':
        return start_date_quarter    
    elif relative_price == 's':
        return start_date_sem
    elif relative_price == 'M':
        return start_date_month
    else:
        return None

def add_hospital_claim_date_filter(claims_queryset, relative_price_g, start_date, start_date_ip, end_date, ceiling_interpreation):
    if relative_price_g or start_date == start_date_ip:
        claims_queryset = claims_queryset.filter(process_stamp__range=[start_date, end_date])
    else: 
        if start_date is not None:
            cond_op = Q(process_stamp__range=[start_date, end_date]) & ~get_hospital_claim_filter(ceiling_interpreation)
        if start_date is not None:
            cond_ip = Q(process_stamp__range=[start_date_ip, end_date]) & get_hospital_claim_filter(ceiling_interpreation)
        if cond_op is not None and cond_ip is not None:
            claims_queryset = claims_queryset.filter(cond_op | cond_ip)
        elif cond_op is not None:
            claims_queryset = claims_queryset.filter(cond_op)
        elif cond_ip is not None:
            claims_queryset = claims_queryset.filter(cond_ip)
        else:
            # kill the queryset because it is not possible
            claims_queryset = claims_queryset.filter(id=-1)

    return claims_queryset


def get_hospital_claim_filter(ci):
    if ci  == Product.CEILING_INTERPRETATION_HOSPITAL:
        return  Q( health_facility_level=HealthFacility.LEVEL_HOSPITAL)
    else:
        return (Q(date_to__is_null=False) & Q(date_to__gt=F('date_from')))


# update the service and item valuated amount 
def claim_batch_valuation(work_data):
    allocated_contributions = work_data["allocated_contributions"]
    allocated_contributions_ip = work_data["allocated_contributions_ip"]
    relative_price_g = work_data["relative_price_g"]
    product = work_data["product"]
    items = work_data["items"]
    services = work_data["services"]
    start_date = work_data["start_date"]
    start_date_ip = work_data["end_date_ip"]
    end_date = work_data["end_date"]
    claims = work_data["claims"]

    # Sum up all item and service amount
    value_hospital = 0
    value_non_hospital = 0
    period_type, period_id = get_period(product, end_date)
    period_type_ip, period_id_ip = get_period(product, end_date)

    # if there is no configuration the relative index will be set to 100 %
    if start_date is not None  or  start_date_ip is not None > 0:
        claims = add_hospital_claim_date_filter(claims, relative_price_g, start_date, start_date_ip, end_date  )
        claims = claims.prefetch_related(Prefetch('items', queryset=ClaimItem.objects.filter(legacy_id__isnull=True).filter(product = product)))\
                .prefetch_related(Prefetch('services', queryset=ClaimService.objects.filter(legacy_id__isnull=True).filter(product = product)))
        claims = list(claims.values_list('id', flat=True).distinct())
        claims = Claim.objects.filter(id__in=claims)
        # TODO to be replace by 2 queryset +  an Annotate add_hospital_claim_date_filter function could be used
        for claim in claims:
            for item in claim.items:
                if is_hospital_claim(product, claim):
                    value_hospital += item.price_valuated if item.price_valuated is not None else 0
                else:
                    value_non_hospital += item.price_valuated if item.price_valuated is not None else 0

            for service in claim.services:
                if is_hospital_claim(product, claim):
                    value_hospital  += service.price_valuated if service.price_valuated is not None else 0
                else:
                    value_non_hospital += service.price_valuated if service.price_valuated is not None else 0
                # calculate the index based on product config

        # create i/o index OR in and out patien index
        if relative_price_g :
            index = get_relative_price_rate(product, 'B', start_date, end_date, allocated_contributions, value_non_hospital + value_hospital) 
        else:
            if start_date_ip is not None:
                index_ip = get_relative_price_rate(product, 'I', start_date, end_date, allocated_contributions, value_non_hospital + value_hospital) 
            elif start_date is not None:
                index = get_relative_price_rate(product, 'O', start_date, end_date, allocated_contributions, value_non_hospital + value_hospital) 

        # update the item and services 
        # TODO check if a single UPDATE query is possible       
        for claim in claims:
            for item in items:
                if is_hospital_claim(work_data.product, item.claim) and (index > 0 or index_ip > 0):
                    item.amount_remunerated = price_valuated * (index if relative_price_g else index_ip)
                    item.save()
                elif index > 0 or index_op > 0:
                    item.amount_remunerated = price_valuated * (index )
                    item.save()
            for service in services:
                if is_hospital_claim(work_data.product, service.claim) and (index>0 or index_ip>0):
                    service.amount_remunerated = price_valuated * (index if relative_price_g else index_ip)
                    service.save()
                elif index > 0 or index_op > 0:
                    service.amount_remunerated = price_valuated * index
                    service.save() 


# to be moded in product services
def is_hospital_claim(product, claim):
    if product.ceiling_interpretation  == Product.CEILING_INTERPRETATION_HOSPITAL:
        return claim.health_facility.level == HealthFacility.LEVEL_HOSPITAL
    else:
        return claim.date_to is not None and claim.date_to > claim.date_from


# to be moded in product services
def create_index(product, index , index_type, period_type, period_id):
    index = RetaliveIndex()
    index.product = product
    #FIXME fill before save

    index.save()
     #just create the row in db


# might be added in product service
def get_relative_price_rate(product, index_type, date_start, end_date, allocated_contributions, sum_r_items_services):
    #FIXME get only the matching row
    rel_distribution = RelativeDistribution.objects.filter(product = product)\
        .filter(period = period)\
        .filter(type = period_type)\
        .filter(care_type = index_type)\
        .filter(legacy_id__isnull =True)
    rel_rate = rel_distribution.percent
    if rel_irel_ratendex is not None:
        index = rel_rate * allocated_contributions / (sum_r_items_services)
        period_type, period_id = get_period(date_start, end_date)
        create_index(product, index , index_type, period_type, period_id)
        return index
    else:
        return 1



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
                    @HFLevel = @HF
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
