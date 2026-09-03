from core.rights_role_test_case import RightsRoleGraphQLTestCase
from core.test_helpers import (
    create_accountant_role,
    create_right_only_user,
    create_role_user,
)
from location.test_helpers import create_basic_test_locations


class ClaimBatchRightsTests(RightsRoleGraphQLTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        create_basic_test_locations()

    def test_query_batch_runs_right(self):
        allowed = create_right_only_user(
            "r_br_q", ["gql_query_batch_runs_perms"], district_codes=self.DISTRICT_CODES
        )
        denied = create_right_only_user("r_br_q_no", [], district_codes=self.DISTRICT_CODES)
        self.assert_user_has_named_perms(allowed, ["gql_query_batch_runs_perms"])
        self.assert_user_lacks_named_perms(denied, ["gql_query_batch_runs_perms"])


class ClaimBatchRoleTests(RightsRoleGraphQLTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        create_basic_test_locations()
        cls.accountant = create_role_user(
            "br_acc", create_accountant_role(), district_codes=cls.DISTRICT_CODES
        )

    def test_accountant_can_query_batch_runs(self):
        self.assert_user_has_named_perms(self.accountant, ["gql_query_batch_runs_perms"])
