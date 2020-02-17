from claim_batch.models import RelativeDistribution


def create_test_rel_distr_range(product_id, dist_type, care_type, percent, custom_props=None):
    if dist_type == 1:
        r = range(1, 13)
    elif dist_type == 4:
        r = [3, 6, 9, 12]
    else:
        r = [12]
    for month in r:
        RelativeDistribution.objects.create(
            **{
                "product_id": product_id,
                "type": dist_type,
                "care_type": care_type,
                "period": month,
                "percent": percent,
                "validity_from": "2019-06-01",
                "audit_user_id": -1,
                **(custom_props if custom_props else {})
            }
        )
