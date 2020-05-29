from claim.gql_mutations import validate_and_process_dedrem_claim
from claim.models import ClaimDedRem, Claim
from claim.test_helpers import (
    create_test_claim,
    create_test_claimservice,
    create_test_claimitem,
    delete_claim_with_itemsvc_dedrem_and_history,
)
from claim_batch.models import RelativeDistribution
from claim_batch.services import do_process_batch
from claim_batch.test_helpers import create_test_rel_distr_range
from contribution.test_helpers import create_test_payer, create_test_premium
from core.models import User, InteractiveUser
from django.test import TestCase
from insuree.test_helpers import create_test_insuree
from medical.test_helpers import create_test_service, create_test_item
from medical_pricelist.test_helpers import (
    add_service_to_hf_pricelist,
    add_item_to_hf_pricelist,
)
from policy.test_helpers import create_test_policy
from product.models import ProductItemOrService, Product
from product.test_helpers import (
    create_test_product,
    create_test_product_service,
    create_test_product_item,
)


class BatchRunTest(TestCase):
    def setUp(self) -> None:
        self.i_user = InteractiveUser(
            login_name="test_batch_run", audit_user_id=-1, id=97891
        )
        self.user = User(i_user=self.i_user)

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
                "period_rel_prices": Product.RELATIVE_PRICE_PERIOD_MONTH,
            },
        )
        create_test_rel_distr_range(
            product.id,
            RelativeDistribution.TYPE_MONTH,
            RelativeDistribution.CARE_TYPE_BOTH,
            10,
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
        policy = create_test_policy(product, insuree, link=True)
        payer = create_test_payer()
        premium = create_test_premium(
            policy_id=policy.id, custom_props={"payer_id": payer.id}
        )
        pricelist_detail1 = add_service_to_hf_pricelist(service)
        pricelist_detail2 = add_item_to_hf_pricelist(item)

        claim1 = create_test_claim({"insuree_id": insuree.id})
        service1 = create_test_claimservice(
            claim1, custom_props={"service_id": service.id, "qty_provided": 2}
        )
        item1 = create_test_claimitem(
            claim1, "A", custom_props={"item_id": item.id, "qty_provided": 3}
        )
        errors = validate_and_process_dedrem_claim(claim1, self.user, True)

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
        do_process_batch(
            self.user.id_for_audit,
            None,
            claim1.process_stamp.month,
            claim1.process_stamp.year,
        )

        claim1.refresh_from_db()
        item1.refresh_from_db()
        service1.refresh_from_db()

        self.assertEquals(claim1.status, Claim.STATUS_VALUATED)

        # tearDown
        # dedrem.delete() # already done if the test passed
        premium.delete()
        payer.delete()
        delete_claim_with_itemsvc_dedrem_and_history(claim1)
        policy.insuree_policies.first().delete()
        policy.delete()
        product_item.delete()
        product_service.delete()
        pricelist_detail1.delete()
        pricelist_detail2.delete()
        service.delete()
        item.delete()
        product.relativeindex_set.all().delete()
        product.relative_distributions.all().delete()
        product.delete()
