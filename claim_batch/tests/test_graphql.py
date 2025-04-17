import json
from dataclasses import dataclass
from core.models import User
from core.models.openimis_graphql_test_case import openIMISGraphQLTestCase, BaseTestContext
from core.test_helpers import create_test_interactive_user
from django.conf import settings
from graphene_django.utils.testing import GraphQLTestCase
from graphql_jwt.shortcuts import get_token
from location.test_helpers import assign_user_districts
from rest_framework import status
from insuree.test_helpers import create_test_insuree
from location.test_helpers import create_test_village
import calendar
import datetime
import uuid

from claim.services import ClaimSubmitService, processing_claim
from claim.test_helpers import (
    create_test_claim,
    create_test_claimservice,
    create_test_claimitem,
)
from contribution.test_helpers import create_test_payer, create_test_premium
from contribution_plan.tests.helpers import create_test_payment_plan
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




# from openIMIS import schema




class ClaimBactchGQLTestCase(openIMISGraphQLTestCase):
    # This is required by some version of graphene but is never used. It should be set to the schema but the import
    # is shown as an error in the IDE, so leaving it as True.
    admin_user = None
    ca_user = None
    ca_token = None
    test_village = None
    test_insuree = None
    test_photo = None
    submit_service = None
    year = None
    month = None
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.test_village = create_test_village()
        cls.test_insuree = create_test_insuree(with_family=True, is_head=True, custom_props={'current_village':cls.test_village}, family_custom_props={'location':cls.test_village})
        cls.admin_user = create_test_interactive_user(username="testLocationAdmin")
        cls.admin_token = BaseTestContext(user=cls.admin_user).get_jwt()
        cls.ca_user = create_test_interactive_user(username="testLocationNoRight", roles=[9])
        cls.ca_token = BaseTestContext(user=cls.ca_user).get_jwt()
        cls.admin_dist_user = create_test_interactive_user(username="testLocationDist")
        assign_user_districts(cls.admin_dist_user, ["R1D1", "R2D1", "R2D2", "R2D1", cls.test_village.parent.parent.code])
        cls.admin_dist_token = BaseTestContext(user=cls.admin_dist_user).get_jwt()
        cls.submit_service = ClaimSubmitService(cls.admin_dist_user)
        insuree = create_test_insuree()
        service = create_test_service("B", custom_props={"name": "test_simple_batch"})
        item = create_test_item("B", custom_props={"name": "test_simple_batch"})
        product = create_test_product(
            "BCUL0002",
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
        policy = create_test_policy(product, insuree, link=True)
        payer = create_test_payer()
        premium = create_test_premium(
            policy_id=policy.id, custom_props={"payer_id": payer.id}
        )
        claim1 = create_test_claim({"insuree_id": insuree.id})
        pricelist_detail1 = add_service_to_hf_pricelist(service, claim1.health_facility_id)
        pricelist_detail2 = add_item_to_hf_pricelist(item, claim1.health_facility_id)
        
        service1 = create_test_claimservice(
            claim1, custom_props={"service_id": service.id, "qty_provided": 2, "price_origin": ProductItemOrService.ORIGIN_RELATIVE}
        )
        item1 = create_test_claimitem(
            claim1, item.type, custom_props={"item_id": item.id, "qty_provided": 3, "price_origin": ProductItemOrService.ORIGIN_RELATIVE}
        )
        claim1.refresh_from_db()
        
        cls.submit_service.submit_claim(claim1, cls.admin_user)
        processing_claim(claim1, cls.admin_user, True)
        _, days_in_month = calendar.monthrange(claim1.validity_from.year, claim1.validity_from.month)
        # add process stamp for claim to not use the process_stamp with now()
        claim1.process_stamp = datetime.datetime(claim1.validity_from.year, claim1.validity_from.month, days_in_month-1)
        cls.year = claim1.validity_from.year
        cls.month = claim1.validity_from.month
        claim1.save()
        
        

    def test_query_insuree_number_validity(self):
        response = self.query(
            f'''

    mutation {{
      processBatch(
        input: {{
          clientMutationId: "82365744-dc14-456e-bac6-109925bf8c7f"
          clientMutationLabel: "Évaluation par lots - National, April 2019"
          
          month: {self.month}
          year: {self.year}
        }}
      ) {{
        clientMutationId
        internalId
        }}
      
    }}
            ''',
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.admin_token}"},
        )

        self.assertEquals(response.status_code, status.HTTP_200_OK)
        self.assertResponseNoErrors(response)
        #TODO mutation should be found 
        #AssertMutation(self,uuid.UUID("82365744-dc14-456e-bac6-109925bf8c7f"), self.admin_dist_token )

