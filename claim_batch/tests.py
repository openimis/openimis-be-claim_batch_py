from claim.models import Claim
from claim.test_helpers import create_test_claim, create_test_claimservice
from claim.validations import validate_claim, validate_assign_prod_to_claimitems_and_services
from claim_batch.models import RelativeDistribution
from claim_batch.services import process_batch
from claim_batch.test_helpers import create_test_rel_distr_range
from contribution.test_helpers import create_test_premium
from django.test import TestCase
from insuree.test_helpers import create_test_insuree
from medical.test_helpers import create_test_service
from medical_pricelist.test_helpers import add_service_to_hf_pricelist
from policy.test_helpers import create_test_policy
from product.models import ProductService
from product.test_helpers import create_test_product, create_test_product_service


def _mark_as_processed(claim):
    claim.status = Claim.STATUS_PROCESSED
    claim.audit_user_id_process = -1
    claim.process_stamp = claim.date_claimed
    claim.save()


class BatchRunTests(TestCase):
    def setUp(self) -> None:
        pass

    def tearDown(self) -> None:
        pass

    def test_simple_monthly_batch_run(self) -> None:
        insuree1 = create_test_insuree()
        self.assertIsNotNone(insuree1)
        product = create_test_product("TEST1", custom_props={"location_id": 1, "period_rel_prices": 4})
        policy = create_test_policy(product, insuree1, link=True)
        service = create_test_service("V")
        product_service = create_test_product_service(product, service,
                                                      custom_props={"price_origin": ProductService.ORIGIN_RELATIVE})
        pricelist_detail = add_service_to_hf_pricelist(service)
        premium = create_test_premium(policy_id=policy.id, with_payer=True)
        create_test_rel_distr_range(product.id, RelativeDistribution.TYPE_QUARTER,
                                    RelativeDistribution.CARE_TYPE_BOTH, 10)

        claim1 = create_test_claim({"insuree_id": insuree1.id})
        service1 = create_test_claimservice(claim1, custom_props={"service_id": service.id,
                                                                  "price_origin": ProductService.ORIGIN_RELATIVE,
                                                                  "product": product})
        errors = validate_claim(claim1, True)
        self.assertEquals(len(errors), 0)
        errors = validate_assign_prod_to_claimitems_and_services(claim1)
        self.assertEquals(len(errors), 0)
        _mark_as_processed(claim1)

        claim1.refresh_from_db()

        result = process_batch(1, location_id=1, period=6, year=2019)

        # tearDown
        premium.delete()
        premium.payer.delete()
        service1.delete()
        claim1.delete()
        policy.insuree_policies.first().delete()
        policy.delete()
        product_service.delete()
        pricelist_detail.delete()
        service.delete()
        for rd in product.relative_distributions.all():
            rd.delete()
        product.delete()
