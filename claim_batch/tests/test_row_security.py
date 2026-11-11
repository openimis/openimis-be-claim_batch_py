"""Row security on batch runs and relative indexes (OP-3322).

A batch run (and the relative index it computes) is visible when its location
sits in the user's districts; no location is the national run, visible to every
reader. Running a batch follows the same scope, and the national run is
reserved to a user whose scope is national.
"""

import datetime
import json

from core.models.openimis_graphql_test_case import (
    BaseTestContext,
    openIMISGraphQLTestCase,
)
from core.test_helpers import create_right_only_user, create_test_interactive_user
from django.conf import settings
from django.core.exceptions import PermissionDenied
from location.models import Location
from location.test_helpers import create_basic_test_locations
from product.test_helpers import create_test_product

from claim_batch.models import BatchRun, RelativeIndex
from claim_batch.schema import ProcessBatchMutation
from claim_batch.services import check_batch_location_scope

BATCH_PERMS = [
    "gql_query_batch_runs_perms",
    "gql_query_relative_indexes_perms",
    "gql_mutation_process_batch_perms",
]


class ClaimBatchRowSecurityTest(openIMISGraphQLTestCase):
    GRAPHQL_URL = f"/{settings.SITE_ROOT()}graphql"
    GRAPHQL_SCHEMA = True

    @classmethod
    def setUpTestData(cls):
        create_basic_test_locations()
        cls.own_district = Location.objects.get(code="R2D1", validity_to__isnull=True)
        cls.other_district = Location.objects.get(code="R1D1", validity_to__isnull=True)
        cls.user = create_right_only_user(
            "batchRowSecUser", BATCH_PERMS, district_codes=["R2D1"]
        )
        cls.token = BaseTestContext(user=cls.user).get_jwt()
        cls.admin = create_test_interactive_user(username="batchRowSecAdmin")
        cls.product = create_test_product("BATRS")
        cls.runs = {
            key: BatchRun.objects.create(
                location=location,
                run_date=datetime.datetime(2020, 1, 31),
                run_year=2020,
                run_month=month,
                audit_user_id=-1,
            )
            for key, location, month in (
                ("own", cls.own_district, 1),
                ("other", cls.other_district, 2),
                ("national", None, 3),
            )
        }
        cls.indexes = {
            key: RelativeIndex.objects.create(
                location=location,
                product=cls.product,
                type=RelativeIndex.TYPE_MONTH,
                care_type=RelativeIndex.CARE_TYPE_BOTH,
                year=2020,
                period=period,
                calc_date=datetime.datetime(2020, 1, 31),
                rel_index=1,
                audit_user_id=-1,
            )
            for key, location, period in (
                ("own", cls.own_district, 1),
                ("other", cls.other_district, 2),
                ("national", None, 3),
            )
        }

    def _gql(self, query, variables=None):
        return self.query(
            query,
            headers={"HTTP_AUTHORIZATION": f"Bearer {self.token}"},
            variables=variables or {},
        )

    # -- model scope ------------------------------------------------------------

    def test_batch_run_scope(self):
        visible = set(
            BatchRun.get_queryset(BatchRun.objects.all(), self.user).values_list("id", flat=True)
        )
        self.assertIn(self.runs["own"].id, visible)
        self.assertIn(self.runs["national"].id, visible)
        self.assertNotIn(self.runs["other"].id, visible)

    def test_relative_index_scope(self):
        visible = set(
            RelativeIndex.get_queryset(RelativeIndex.objects.all(), self.user)
            .values_list("id", flat=True)
        )
        self.assertIn(self.indexes["own"].id, visible)
        self.assertIn(self.indexes["national"].id, visible)
        self.assertNotIn(self.indexes["other"].id, visible)

    # -- GraphQL reads ----------------------------------------------------------

    def test_batch_runs_connection_is_scoped(self):
        response = self._gql("{ batchRuns { edges { node { runMonth } } } }")
        self.assertResponseNoErrors(response)
        months = {
            edge["node"]["runMonth"]
            for edge in json.loads(response.content)["data"]["batchRuns"]["edges"]
        }
        self.assertIn(1, months)
        self.assertIn(3, months)
        self.assertNotIn(2, months)

    def test_relative_indexes_connection_is_scoped(self):
        response = self._gql("{ relativeIndexes { edges { node { period } } } }")
        self.assertResponseNoErrors(response)
        periods = {
            edge["node"]["period"]
            for edge in json.loads(response.content)["data"]["relativeIndexes"]["edges"]
        }
        self.assertIn(1, periods)
        self.assertIn(3, periods)
        self.assertNotIn(2, periods)

    def test_summaries_for_a_district_outside_scope_are_refused(self):
        response = self._gql(
            "query ($d: Int) { batchRunsSummaries(accountDistrict: $d) { totalCount } }",
            {"d": self.other_district.id},
        )
        self.assertResponseHasErrors(response)

    def test_summaries_for_a_region_outside_scope_are_refused(self):
        response = self._gql(
            "query ($r: Int) { batchRunsSummaries(accountRegion: $r) { totalCount } }",
            {"r": self.other_district.parent_id},
        )
        self.assertResponseHasErrors(response)

    def test_summaries_for_own_district_are_served(self):
        response = self._gql(
            "query ($d: Int) { batchRunsSummaries(accountDistrict: $d) { totalCount } }",
            {"d": self.own_district.id},
        )
        self.assertResponseNoErrors(response)

    # -- processBatch -----------------------------------------------------------

    def test_batch_for_own_district_is_allowed(self):
        check_batch_location_scope(self.user, self.own_district.id)

    def test_batch_for_another_district_is_refused(self):
        with self.assertRaises(PermissionDenied):
            check_batch_location_scope(self.user, self.other_district.id)

    def test_national_batch_is_refused_to_a_district_user(self):
        for national in (None, -1):
            with self.subTest(location_id=national):
                with self.assertRaises(PermissionDenied):
                    check_batch_location_scope(self.user, national)

    def test_national_batch_is_allowed_to_a_superuser(self):
        check_batch_location_scope(self.admin, None)
        check_batch_location_scope(self.admin, -1)

    def test_process_batch_mutation_refuses_another_district(self):
        with self.assertRaises(PermissionDenied):
            ProcessBatchMutation.async_mutate(
                self.user, location_id=self.other_district.id, year=2021, month=5
            )
        self.assertFalse(
            BatchRun.objects.filter(run_year=2021, run_month=5).exists()
        )
