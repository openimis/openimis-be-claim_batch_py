import calendar
import datetime
import logging
import pandas as pd
from django.contrib.admin.options import get_content_type_for_model
from django.contrib.contenttypes.models import ContentType

import core

from django.db import connection, transaction
from django.db.models import Value, F, Q, Subquery, TextField
from django.db.models.functions import Coalesce, Cast
from django.utils.translation import gettext as _

from calculation.services import get_calculation_object
from claim.models import ClaimItem, Claim, ClaimService
from claim_batch.models import BatchRun, RelativeIndex
from contribution.models import Premium
from contribution_plan.models import PaymentPlan
from invoice.models import BillPayment, InvoicePayment
from location.models import HealthFacility, Location
from product.models import Product
from functools import lru_cache
from claim_batch.signals import batch_run_determine_products
from claim.subqueries import (
    update_claim_valuated as claim_update_claim_valuated,
    update_claim_indexed_remunerated as claim_update_claim_indexed_remunerated
)
logger = logging.getLogger(__name__)


@lru_cache(maxsize=None)
def product_content_type():
    # Wrapped in function as property is not compliant with type and static variable fails migrations.
    return ContentType.objects.get_for_model(Product)


@core.comparable
class ProcessBatchSubmit(object):
    def __init__(self, location_id=None, year=None, month=None, scope=None):
        self.location_id = location_id
        self.scope = scope
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
        scope = getattr(submit, 'scope', None) or submit.location_id
        return process_batch(self.user.i_user.id, scope=scope, period=submit.month, year=submit.year)


def batch_run_already_executed(cls, year, month, location_id=None, scope=None):
    qs = BatchRun.objects.filter(*BatchRun.filter_validity(), run_year=year, run_month=month)

    if scope is not None:
        # New GFK path
        ct = ContentType.objects.get_for_model(scope.__class__)
        qs = qs.filter(scope_type=ct, scope_id=str(scope.pk))
    elif location_id is not None:
        # Legacy / scope as location
        if location_id == -1:
            location_id = None
        if location_id is None:
            qs = qs.filter(scope_id__isnull=True) | qs.filter(scope__isnull=True)
        else:
            # Try both the new GFK (when scope is a Location) and old-style if still present
            loc_ct = ContentType.objects.get_for_model(Location)
            qs = qs.filter(
                Q(scope_type=loc_ct, scope_id=str(location_id))
                | Q(scope_id=str(location_id))  # fallback if only id was stored
            )
    else:
        # No scope info -> consider only null scope runs
        qs = qs.filter(scope_id__isnull=True)

    return qs.exists()


def _resolve_scope(scope_or_location_id):
    """Turn a legacy location_id or a scope object into a usable scope (or None)."""
    if scope_or_location_id is None:
        return None
    if isinstance(scope_or_location_id, (Location, Product)):
        return scope_or_location_id
    # assume it's a location id (int or -1)
    if scope_or_location_id == -1:
        return None
    try:
        return Location.objects.get(id=scope_or_location_id)
    except (Location.DoesNotExist, ValueError, TypeError):
        return None


def get_products_for_batch_run(batch_run, end_date):
    """Return list of Products for this BatchRun based on its scope + json_ext + signals."""
    if not batch_run:
        return []

    scope = batch_run.scope

    # 1. Direct Product scope
    if isinstance(scope, Product):
        return [scope]

    # 2. Location scope (or legacy location)
    if isinstance(scope, Location):
        return get_product_queryset(end_date, scope.id)

    # 3. Products already stored in json_ext (set by previous run or receiver)
    prods = batch_run.products
    if prods:
        return prods

    # 4. Neither Product nor Location -> give receivers a chance to populate
    batch_run_determine_products.send(sender=BatchRun, batch_run=batch_run)

    # Receivers may have done batch_run.products = [...] (which writes json_ext)
    prods = batch_run.products
    if prods:
        # persist what the receivers decided
        batch_run.save(update_fields=['json_ext'] if hasattr(batch_run, 'json_ext') else None)
        return prods

    # 5. Fallback to old location-based logic if scope is None (null-scope run)
    return get_product_queryset(end_date, None)


@transaction.atomic
def process_batch(audit_user_id, scope=None, period=None, year=None, location_id=None):
    """Process a batch run.

    scope can be:
      - a Location instance (legacy behaviour)
      - a Product instance (direct product batch)
      - any other model instance (will trigger batch_run_determine_products signal)
      - None (global / null scope)

    For backward compatibility, location_id is still accepted and converted to a Location scope.
    """
    already_run_batch = batch_run_already_executed(year, period, location_id, scope)
    if already_run_batch:
        return [str(ProcessBatchSubmitError(2))]

    _, days_in_month = calendar.monthrange(year, period)
    end_date = (
        datetime.datetime(year, period, days_in_month)
        + datetime.timedelta(days=1)
    )

    try:
        logger.debug("do_process_batch scope=%s for %s/%s", scope, period, year)

        from core.utils import TimeUtils
        created_run = BatchRun(
            run_year=year,
            run_month=period,
            run_date=TimeUtils.now(),
            audit_user_id=audit_user_id,
            validity_from=TimeUtils.now(),
            scope=scope,
        )
        created_run.save()
        logger.debug(f"do_process_batch created run: {created_run.id}")

        products = get_products_for_batch_run(created_run, end_date)

        if products:
            if isinstance(scope, Location):
                for product in products:
                    do_process_batch(audit_user_id, scope, [product], end_date, created_run)
            else:
                do_process_batch(audit_user_id, scope, products, end_date, created_run)

    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as exc:
        logger.warning(
            f"Exception while processing batch user {audit_user_id}, scope {scope}, period {period}, year {year}",
            exc_info=True
        )
        return [str(ProcessBatchSubmitError(-1, str(exc)))]



def do_process_batch(audit_user_id, scope, products, end_date, created_run):
    # As we update claims, we add the claims not in relative pricing and then update the status

    period = end_date.month
    year = end_date.year
    logger.debug("do_process_batch location %s for %s/%s",
                 location_id, period, year)

    from core.utils import TimeUtils
    created_run = BatchRun.objects.create(location_id=location_id, run_year=year, run_month=period,
                                          run_date=TimeUtils.now(), audit_user_id=audit_user_id,
                                          *BatchRun.filter_validity())
    logger.debug(f"do_process_batch created run: {created_run.id}")

    # 0 prepare the batch run :  does it really make sense
    # per location ? (Ideally per pool but the notion doesn't exist yet)
    # 0.1 get all product concerned, all product that have are configured for the location
    # period_quarter = period - 2 if period % 3 == 0 else 0
    # period_sem = period - 5 if period % 6 == 0 else 0

    products = get_product_queryset(end_date, location_id)
    # 1 per product (Ideally per pool but the notion doesn't exist yet)
    if products:
        product_list = _as_product_list(products)
        logger.debug(
            f"do_process_batch creating batch run process for products {[p.code for p in product_list]}")
        work_data = {
            "created_run": created_run,
            "products": product_list,  # use only products (list)
            "end_date": end_date,
        }
        if created_run.scope:
            work_data["scope"] = created_run.scope
        allocated_contribution = None
        # 1.2 get all the payment plan per product
        payment_plans = []
        for prod in product_list:
            payment_plans.extend(get_payment_plan_queryset(prod, end_date))
        work_data["payment_plans"] = payment_plans
        logger.debug(
            f"{len(work_data['payment_plans'])} payment plan found")
        # valuate the claims
        # 5 Generate BatchPayment per product (Ideally per pool but the notion doesn't exist yet)
        trigger_calculation_based_on_context(
            "BatchValuate",
            work_data,
            Claim.STATUS_PROCESSED,
            end_date,
            allocated_contribution,
            audit_user_id
        )
        # 5.1 filter a calculation valid for batchRun with context BatchPayment (got via 0.2)
        # 54.2 Execute the converter per product/batch run/claim (not claims)
        trigger_calculation_based_on_context(
            "BatchPayment",
            work_data,
            Claim.STATUS_VALUATED,
            end_date,
            allocated_contribution,
            audit_user_id
        )
        # save the batch run into db
        logger.debug("do_process_batch created run: %s", created_run.id)
    else:
        logger.info("no product found for batch")
    return created_run


def add_status_filter(work_data, status):
    ret = work_data.copy()
    ret['claims'] = work_data['claims'].filter(status=status)
    ret['items'] = work_data['items'].filter(claim_status=status)
    ret['services'] = work_data['services'].filter(claim_status=status)
    return ret


def trigger_calculation_based_on_context(
        context, work_data, status, end_date, allocated_contribution, user_id
):
    """Trigger calc rules for BatchValuate / BatchPayment etc.
    Work data is expected to carry 'products' (list) rather than location_id or single product.
    """
    if work_data.get("payment_plans"):

        for payment_plan in work_data["payment_plans"]:
            logger.debug(
                f"Starting evaluating payment plan {payment_plan.code}")
            start_date = get_start_date(end_date, payment_plan.periodicity)
            # run only when it makes sense based on periodicitiy
            if start_date is not None:
                # Scope to this payment plan's specific product (benefit_plan).
                # This ensures that even when do_process_batch is called with a list of
                # products, each payment plan's valuation/payment works against only its
                # own product's claims/items/etc. We copy the dict so we don't mutate
                # the outer work_data for other plans in the same batch.
                plan_product = getattr(payment_plan, "benefit_plan", None)
                if plan_product:
                    work_data = dict(work_data)
                    work_data["products"] = [plan_product]
                allocated_contribution, work_data = update_work_data(
                    work_data, status, start_date, end_date, allocated_contribution
                )
                calculation = get_calculation_object(payment_plan.calculation)
                if calculation is not None:
                    try:
                        rcr = calculation.calculate_if_active_for_object(
                            payment_plan, context=context,
                            work_data=work_data, audit_user_id=user_id,
                            start_date=start_date, end_date=end_date
                        )
                        if rcr:
                            logger.debug(
                                "conversion processed for: %s", str(rcr))
                        else:
                            logger.debug(
                                f"No conversion done for {payment_plan.code}")
                    except Exception as e:
                        message = _(
                            "Batch run %s failed %s: %s" % (
                                calculation.calculation_rule_name,
                                context,
                                str(e)
                            )
                        )
                        logger.debug(message)
                        raise Exception(message)
                else:
                    logger.debug(
                        f"Calulation not found for {payment_plan.code}")


def _as_product_list(val):
    """Normalize product or list of products to list. Supports 'products' in work_data or legacy 'product'."""
    if val is None:
        return []
    if isinstance(val, (list, tuple, set)):
        return [p for p in val if p]
    return [val]


def update_work_data(work_data, status, start_date, end_date, allocated_contribution=None):
    work_data["start_date"] = start_date
    # adapt work data: use only products list (with legacy fallback)
    products = get_products_from_work_data(work_data)
    # keep normalized
    work_data["products"] = products
# 1.3 generate queryset
    work_data["items"] = get_items_queryset(
        products, status, work_data.get('created_run'), start_date, end_date)
    work_data["services"] = get_services_queryset(
        products, status, work_data.get('created_run'), start_date, end_date)
    work_data["contributions"] = get_contribution_queryset(
        products, start_date, end_date)
    work_data['claims'] = get_claim_queryset(
        products, status, work_data.get('created_run'), start_date, end_date)
    work_data['bill_payments'] = get_bill_payment_queryset(
        products, start_date, end_date)

    work_data['invoice_payments'] = get_invoice_payment_queryset(
        products, start_date, end_date)
    if allocated_contribution is None:
        allocated_contribution = {}
    start_date_str = str(start_date)
    if start_date_str not in allocated_contribution:
        allocated_contribution[start_date_str] = get_allocated_premium(
            get_allocated_contribution_queryset(products, start_date, end_date), start_date, end_date)
    work_data['allocated_contributions'] = allocated_contribution[start_date_str]
    return allocated_contribution, work_data


def get_payment_plan_queryset(products, end_date):
    products = _as_product_list(products)
    if not products:
        return PaymentPlan.objects.none()
    return PaymentPlan.objects.filter(
        Q(date_valid_to__isnull=True) | Q(date_valid_to__gte=end_date),
        date_valid_from__lte=end_date,
        benefit_plan_id__in=[p.id for p in products],
        benefit_plan_type=product_content_type()
    ).filter(is_deleted=False)


def get_items_queryset(products, status, batch_run, start_date, end_date):
    products = _as_product_list(products)
    if not products:
        return ClaimItem.objects.none()
    subquery = ClaimItem.objects.filter(
        Q(claim__batch_run__isnull=True) | Q(claim__batch_run=batch_run),
        *Claim.filter_validity(prefix='claim__'),
        *ClaimItem.filter_validity(),
        claim__status=status,
        claim__process_stamp__lte=end_date,
        product__in=products
    ).distinct().values('id')

    return ClaimItem.objects.filter(
        id__in=Subquery(subquery)
    ).select_related(
        'claim__health_facility'
    ).order_by('claim__health_facility').order_by('claim')


def get_services_queryset(products, status, batch_run, start_date, end_date):
    products = _as_product_list(products)
    if not products:
        return ClaimService.objects.none()
    subquery = ClaimService.objects.filter(
        Q(claim__batch_run__isnull=True) | Q(claim__batch_run=batch_run),
        *Claim.filter_validity(prefix='claim__'),
        *ClaimService.filter_validity(),
        claim__status=status,
        claim__process_stamp__lte=end_date,
        product__in=products
    ).distinct().values('id')
    return ClaimService.objects.filter(
        id__in=Subquery(subquery)
    ).select_related(
        'claim__health_facility'
    ).order_by('claim__health_facility').order_by('claim')


def get_claim_queryset(products, status, batch_run, start_date, end_date):
    products = _as_product_list(products)
    if not products:
        return Claim.objects.none()
    subquery = Claim.objects.filter(
        Q(items__product__in=products) | Q(services__product__in=products),
        Q(batch_run__isnull=True) | Q(batch_run=batch_run),
        Q(Q(date_to__lt=end_date) | (
            Q(date_to__isnull=True) & Q(date_from__lt=end_date))),
        *Claim.filter_validity(),
        status=status,
        process_stamp__isnull=False,
    ).distinct().values('id')
    return Claim.objects.filter(id__in=Subquery(subquery))


def get_allocated_contribution_queryset(products, start_date, end_date):
    products = _as_product_list(products)
    if not products:
        return Premium.objects.none()
    return Premium.objects.filter(
        *Claim.filter_validity(),
        policy__effective_date__lte=end_date,
        policy__expiry_date__gte=start_date,
        policy__product__in=products
    ).select_related('policy')


def get_product_queryset(end_date, location_id):
    queryset = Product.objects.filter(
        Q(date_to__gte=end_date) | Q(date_to__isnull=True),
        *Product.filter_validity(),
        date_from__lte=end_date,

    )
    if location_id is not None:
        return queryset.filter(location_id=location_id)
    else:
        return queryset.filter(location_id__isnull=True)


def get_contribution_queryset(products, start_date, end_date):
    products = _as_product_list(products)
    if not products:
        return Premium.objects.none()
    return Premium.objects \
        .filter(
            *Premium.filter_validity(),
            created_date__lte=end_date,
            policy__effective_date__lte=end_date,
            policy__expiry_date__gte=start_date,
            policy__product__in=products)


def get_bill_payment_queryset(products, start_date, end_date):
    # need to get the invoice with lines that match premium for that product
    products = _as_product_list(products)
    if not products:
        return BillPayment.objects.none()
    qs = BillPayment.objects.filter(is_deleted=False)\
        .filter(
            date_created__gte=start_date,
            date_created__lt=end_date,
            bill__line_items_bill__line_type=get_content_type_for_model(
                Premium),
            bill__line_items_bill__line_id__in=Subquery(
                Premium.objects.filter(*Premium.filter_validity())
                .filter(policy__product__in=products)
                .annotate(as_str=Cast('id', TextField())).values('as_str')
            )
    )
    return qs


def get_invoice_payment_queryset(products, start_date, end_date):
    products = _as_product_list(products)
    if not products:
        return InvoicePayment.objects.none()
    qs = InvoicePayment.objects.filter(is_deleted=False)\
        .filter(
            date_created__gte=start_date,
            date_created__lt=end_date,
            invoice__line_items__line_type=get_content_type_for_model(Premium),
            invoice__line_items__line_id__in=Subquery(
                Premium.objects.filter(*Premium.filter_validity(),)
                .filter(policy__product__in=products)
                .annotate(as_str=Cast('id', TextField())).values('as_str')
            )
    )
    return qs


def get_allocated_premium(premiums, start_date, end_date):
    # Calculate allcated contributions
    # go trough the contribution and find the allocated contribution
    allocated_premiums = 0
    for premium in premiums:
        # FIXME migration contribution 0008 created_date from date to datetime
        # not working in PSQL for no apparent reason, hence this work arround:
        created_date = premium.created_date.date() if hasattr(
            premium.created_date, 'date') else premium.created_date
        policy_payment_start = max(premium.policy.effective_date, created_date)
        allocation_start = max(policy_payment_start, start_date)
        if isinstance(allocation_start, datetime.datetime):
            allocation_start = allocation_start.date()
        allocation_stop = min(end_date, premium.policy.expiry_date)
        if isinstance(allocation_stop, datetime.datetime):
            allocation_stop = allocation_stop.date()
        allocation_diff = (allocation_stop - allocation_start).days + 1

        policy_duration = (
            premium.policy.expiry_date
            - policy_payment_start
        ).days + (1 if policy_payment_start >= start_date else 0)
        allocated_premiums += premium.amount * allocation_diff / policy_duration
    return allocated_premiums


def get_hospital_claim_filter(ceiling_interpretation, mode='I', prefix=''):
    # return the filter base on cieling interpretation and mode (I inpatient, O outpatient),
    # prefix is required if the queryset is not about claims
    if ceiling_interpretation == Product.CEILING_INTERPRETATION_HOSPITAL:
        Qterm = (Q(('%shealth_facility_level' %
                 prefix, HealthFacility.LEVEL_HOSPITAL)))
    else:
        Qterm = (Q('%sdate_to__isnull' % prefix, False)
                 & Q('%sdate_to__gt' % prefix, F('date_from')))
    if mode == 'I':
        return Qterm
    elif mode == 'O':
        return ~Qterm
    else:
        return Q()


def combine_product_filters(products, lifter):
    """Combine per-product filters using OR (|).

    `lifter` is a callable (lambda or function) that receives a single Product
    and must return a Q() object representing the filter condition specific to
    that product (e.g. a ceiling interpretation based condition, optionally
    combined with a product-scoping Q like Q(product=p) or
    Q(items__product=p) | Q(services__product=p)).

    The result is a Q() that matches records for *any* of the products when
    the record satisfies the lifter condition for its associated product.
    This allows using different product attributes (like ceiling_interpretation)
    in one filter expression.
    """
    products = _as_product_list(products)
    if not products:
        return Q()
    result = None
    for p in products:
        part = lifter(p)
        if result is None:
            result = part
        else:
            result = result | part
    return result or Q()


def get_products_from_work_data(work_data):
    """Return a normalized list of products from work_data.

    Supports the new 'products' (list) key (for flexible batching by product
    instead of location_id) with fallback to legacy single 'product'.
    Always returns a (possibly empty) list.
    """
    if not work_data:
        return []
    prods = work_data.get("products") or work_data.get("product")
    return _as_product_list(prods)


def get_period(start_date, end_date):
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
    if periodicity == 12:
        # yearly
        return datetime.date(year, 1, 1) if month == 12 else None
    elif periodicity == 6:
        # semester
        return datetime.date(year, month - 5, 1) if month % 6 == 0 else None
    elif periodicity == 4:
        # quarter
        return datetime.date(year, month - 4, 1) if month % 4 == 0 else None
    elif periodicity == 3:
        # quarter
        return datetime.date(year, month - 2, 1) if month % 3 == 0 else None
    elif periodicity == 2:
        # quarter
        return datetime.date(year, month - 1, 1) if month % 2 == 0 else None
    elif periodicity == 1:
        # monthy
        return datetime.date(year, month, 1)
    else:
        return None


def update_claim_valuated(claims, batch_run, claim_based_value_subquery=0):
    claim_update_claim_valuated(
        claims,
        claim_based_value_subquery=claim_based_value_subquery,
        updates={'batch_run': batch_run}
    )
    # 4 update the claim Total amounts if all Item and services got "valuated"


def update_claim_indexed_remunerated(claims, batch_run, index=1, claim_based_value_subquery=0):
    claim_update_claim_indexed_remunerated(
        claims,
        ratio=index,
        claim_based_value_subquery=claim_based_value_subquery,
        updates={'batch_run': batch_run}
    )
    # 4 update the claim Total amounts if all Item and services got "valuated"


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
        _execute_capitation_payment_procedure(
            cur, 'uspCreateCapitationPaymentReportData', params)


def get_commision_payment_report_data(params):
    with connection.cursor() as cur:
        # HFLevel based on
        # https://github.com/openimis/web_app_vb/blob/2492c20d8959e39775a2dd4013d2fda8feffd01c/IMIS_BL/HealthFacilityBL.vb#L77
        _execute_capitation_payment_procedure(
            cur, 'uspSSRSRetrieveCapitationPaymentReportData', params)

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


def get_contribution_index_rate(value, pp_params, work_data):
    # capitation_index = weight_of_claim_adjusted_anount / 100 * share of contrib(PP, one per month) *
    # allocated_contribution : / Sum of adjusted_amount for item and services for
    # the product and perdiod (fee for service takes only 'R' price_origin items and services)
    # get distr for the current month
    allocated_contributions = float(work_data["allocated_contributions"])

    weight_adjusted_amount = float(
        pp_params.get("weight_adjusted_amount", 100) / 100)
    value = float(value)
    if value > 0 and allocated_contributions > 0 and 'distr_%i' % work_data['end_date'].month in pp_params:
        distr = float(pp_params['distr_%i' %
                      work_data['end_date'].month] / 100)
        index = (weight_adjusted_amount * distr
                 * allocated_contributions) / value
        period_type, period_id = get_period(
            work_data['start_date'], work_data['end_date'])
        year = work_data['end_date'].year
        audit_user_id = work_data['created_run'].audit_user_id
        prods = get_products_from_work_data(work_data)
        prod = prods[0] if prods else None
        create_index(
            prod, index, pp_params['claim_type'],
            period_type, period_id, year, audit_user_id
        )
        return index, distr
    else:
        return 1, 1


def create_index(product, index_value, index_type, period_type, period_id, year, audit_user_id):
    index = RelativeIndex()
    index.product = product
    index.type = period_type
    index.care_type = index_type
    index.period = period_id
    index.rel_index = index_value
    index.year = year
    index.audit_user_id = audit_user_id
    from core.utils import TimeUtils
    index.calc_date = TimeUtils.now()
    index.save()
