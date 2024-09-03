import calendar
import datetime

from claim.services import ClaimSubmitService, processing_claim
from claim.models import ClaimDedRem, Claim
from claim.test_helpers import (
    create_test_claim,
    create_test_claimservice,
    create_test_claimitem,
    delete_claim_with_itemsvc_dedrem_and_history,
)
from claim_batch.services import do_process_batch, process_batch
from claim_batch.models import BatchRun
from contribution.test_helpers import create_test_payer, create_test_premium
from contribution_plan.models import PaymentPlan
from contribution_plan.tests.helpers import create_test_payment_plan
from core.services import create_or_update_interactive_user, create_or_update_core_user
from django.test import TestCase
from insuree.test_helpers import create_test_insuree
from medical.test_helpers import create_test_service, create_test_item
from medical_pricelist.test_helpers import (
    add_service_to_hf_pricelist,
    add_item_to_hf_pricelist,
)
from policy.test_helpers import create_test_policy
from product.test_helpers import (
    create_test_product,
    create_test_product_service,
    create_test_product_item,
)
from product.models import ProductItemOrService

from core.test_helpers import create_test_interactive_user


class BatchRunTest(TestCase):
    def setUp(self) -> None:
        super(BatchRunTest, self).setUp()
        user = create_test_interactive_user(username="testbatchrun")

        self.user = user
        self.submit_service = ClaimSubmitService(self.user)

    def test_simple_batch(self):
        """
        This test creates a claim, submits it so that it gets dedrem entries,
        then submits a review rejecting part of it, then process the claim.
        It should not be processed (which was ok) but the dedrem should be deleted.
        """
        # Given
        insuree = create_test_insuree()
        self.assertIsNotNone(insuree)
        service = create_test_service("A", custom_props={"name": "test_simple_batch"})
        item = create_test_item("A", custom_props={"name": "test_simple_batch"})

        product = create_test_product(
            "BCUL0001",
            custom_props={
                "name": "simplebatch",
                "lump_sum": 10_000,
            },
        )
        payment_plan = create_test_payment_plan(
            product=product,
            calculation="0a1b6d54-eef4-4ee6-ac47-2a99cfa5e9a8",
            custom_props={
                'periodicity': 1,
                'date_valid_from': '2019-01-01', 
                'date_valid_to': '2050-01-01',
                'json_ext': {
                    'calculation_rule': {
                        'hf_level_1': 'H',
                        'hf_sublevel_1': "null",
                        'hf_level_2': 'D',
                        'hf_sublevel_2': "null",
                        'hf_level_3': 'C',
                        'hf_sublevel_3': "null",
                        'hf_level_4': "null",
                        'hf_sublevel_4': "null",
                        'distr_1': 100,
                        'distr_2': 100,
                        'distr_3': 100,
                        'distr_4': 100,
                        'distr_5': 100,
                        'distr_6': 100,
                        'distr_7': 100,
                        'distr_8': 100,
                        'distr_9': 100,
                        'distr_10': 100,
                        'distr_11': 100,
                        'distr_12': 100,
                        'claim_type': 'B'
                    }
                }
            }
        )

        product_service = create_test_product_service(
            product,
            service,
            custom_props={"price_origin": ProductItemOrService.ORIGIN_RELATIVE},
        )
        product_item = create_test_product_item(
            product,
            item,
            custom_props={"price_origin": ProductItemOrService.ORIGIN_RELATIVE},
        )
        
        now = datetime.datetime.now()
        policy = create_test_policy(product, insuree, link=True, custom_props={
            'expiry_date': datetime.date(now.year, 12, 31),
            'enroll_date': datetime.date(now.year, 1, 1),
            'effective_date': datetime.date(now.year, 1, 1),
            'start_date': datetime.date(now.year, 1, 1),
            })
        
        payer = create_test_payer()
        premium = create_test_premium(
            policy_id=policy.id, custom_props={"payer_id": payer.id}
        )
        pricelist_detail1 = add_service_to_hf_pricelist(service)
        pricelist_detail2 = add_item_to_hf_pricelist(item)

        claim1 = create_test_claim({"insuree_id": insuree.id},
                                   )

        service1 = create_test_claimservice(
            claim1, custom_props={"service_id": service.id, "qty_provided": 2, "price_origin": ProductItemOrService.ORIGIN_RELATIVE}
        )
        item1 = create_test_claimitem(
            claim1, "A", custom_props={"item_id": item.id, "qty_provided": 3, "price_origin": ProductItemOrService.ORIGIN_RELATIVE}
        )
        claim1.refresh_from_db()
        errors = []
        subm_claim, error = self.submit_service.submit_claim(claim1, self.user)
        errors += error
        errors += processing_claim(claim1, self.user, True)
        _, days_in_month = calendar.monthrange(claim1.validity_from.year, claim1.validity_from.month)
        # add process stamp for claim to not use the process_stamp with now()
        claim1.process_stamp = datetime.datetime(claim1.validity_from.year, claim1.validity_from.month, days_in_month-1)
        claim1.save()

        self.assertEqual(len(errors), 0)
        self.assertEqual(
            claim1.status,
            Claim.STATUS_PROCESSED,
            "The claim has relative pricing, so should go to PROCESSED rather than VALUATED",
        )
        # Make sure that the dedrem was generated
        dedrem = ClaimDedRem.objects.filter(claim=claim1).first()
        self.assertIsNotNone(dedrem)
        self.assertEquals(dedrem.rem_g, 500)  # 100*2 + 100*3

        # When
        end_date = datetime.datetime(claim1.validity_from.year, claim1.validity_from.month, days_in_month)
        # run batch (audit_user_id, location_id, period, year):
        Claim.objects.filter(batch_run_id__isnull=False).update(batch_run_id=None)
        BatchRun.objects.all().delete()
        process_batch(
            self.user.id_for_audit,
            product.location_id,
            claim1.validity_from.month,
            claim1.validity_from.year
        )

        claim1.refresh_from_db()
        item1.refresh_from_db()
        service1.refresh_from_db()

        self.assertEquals(claim1.status, Claim.STATUS_VALUATED)

        # tearDown
        # dedrem.delete() # already done if the test passed
