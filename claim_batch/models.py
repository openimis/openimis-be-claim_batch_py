import uuid

from core import fields

from django.contrib.contenttypes.fields import GenericForeignKey
from django.contrib.contenttypes.models import ContentType
from django.db import models
from django.utils.translation import gettext_lazy
from location import models as location_models
from location.models import HealthFacility
from product import models as product_models
from product.models import Product
from core import models as core_models
from core.utils import uuidv7

_location_cs = None


def get_location_content_type_id():
    global _location_cs
    if not _location_cs:
        _location_cs = ContentType.objects.filter(
            app_label='location',
            model='location'
        ).first()

    return _location_cs.id if _location_cs else None


class BatchRun(core_models.OpenIMISModel):
    id = models.AutoField(db_column='RunID', primary_key=True)
    uuid = models.UUIDField(
        unique=True,
        db_column="UUID",
        default=uuidv7,
        editable=False,
        null=True,
    )
    # Generic scope (preferred): Location, Product or custom via signal.
    # The migration sets ScopeType defaulting to Location's ContentType.
    scope_type = models.ForeignKey(
        ContentType,
        db_column="ScopeType",
        on_delete=models.DO_NOTHING,
        unique=False,
        null=True,
        default=get_location_content_type_id
    )

    scope_id = models.CharField(
        db_column="ScopeId",
        max_length=255,
        blank=True,
        null=True)
    scope = GenericForeignKey('scope_type', 'scope_id')
    run_date = fields.DateTimeField(db_column='RunDate')
    audit_user_id = models.IntegerField(db_column='AuditUserID')
    run_year = models.IntegerField(db_column='RunYear')
    run_month = models.SmallIntegerField(db_column='RunMonth')

    class Meta:
        managed = True
        db_table = 'tblBatchRun'

    # --- Products stored in json_ext (no dedicated m2m / list column) ---
    @property
    def products(self):
        """Return list of Product instances.
        Priority:
        1. If scope is a Product -> return it directly.
        2. Products stored in json_ext['products'] (list of ids or dicts).
        """
        # Direct Product scope takes precedence
        if self.scope and isinstance(self.scope, Product):
            return [self.scope]

        if not self.json_ext:
            return []
        raw = self.json_ext.get('products') or []
        if not raw:
            return []
        ids = []
        for item in raw:
            if isinstance(item, dict):
                pid = item.get('id') or item.get('pk') or item.get('product_id')
            else:
                pid = item
            if pid:
                ids.append(pid)
        if not ids:
            return []
        return list(Product.objects.filter(id__in=ids))

    @products.setter
    def products(self, value):
        """Serialize list of Products (or ids) into json_ext['products']."""
        if not value:
            serialised = []
        else:
            serialised = []
            for p in value:
                if hasattr(p, 'id'):  # Product instance or similar
                    serialised.append({
                        'id': p.id,
                        'code': getattr(p, 'code', None),
                        'name': getattr(p, 'name', None),
                    })
                else:
                    serialised.append({'id': p})
        if self.json_ext is None:
            self.json_ext = {}
        self.json_ext['products'] = serialised

    @property
    def location(self):
        """Compatibility property.
        Returns the scope object if it is a Location (for legacy code paths).
        """
        if self.scope and isinstance(self.scope, location_models.Location):
            return self.scope
        return None

    @location.setter
    def location(self, value):
        """Allow assignment for legacy paths; delegates to scope."""
        self.scope = value

    def save(self, *args, **kwargs):
        # Ensure json_ext is a dict
        if self.json_ext is None:
            self.json_ext = {}

        # Sync scope <-> legacy location when one is set
        if self.scope and not self.scope_type:
            try:
                self.scope_type = ContentType.objects.get_for_model(self.scope.__class__)
                self.scope_id = str(getattr(self.scope, 'pk', self.scope))
            except Exception:
                pass
        if (not self.scope) and self.location:
            self.scope = self.location

        super().save(*args, **kwargs)


class RelativeIndex(core_models.OpenIMISModel):
    id = models.AutoField(db_column='RelIndexID', primary_key=True)
    product = models.ForeignKey(
        product_models.Product, models.DO_NOTHING, db_column='ProdID')
    type = models.SmallIntegerField(db_column='RelType')
    care_type = models.CharField(db_column='RelCareType', max_length=1)
    year = models.IntegerField(db_column='RelYear')
    period = models.SmallIntegerField(db_column='RelPeriod')
    calc_date = models.DateTimeField(db_column='CalcDate')
    rel_index = models.DecimalField(
        db_column='RelIndex', max_digits=18, decimal_places=4, blank=True, null=True)
    audit_user_id = models.IntegerField(db_column='AuditUserID')
    # Generic scope (preferred): Location, Product or custom via signal.
    # The migration sets ScopeType defaulting to Location's ContentType.
    scope_type = models.ForeignKey(
        ContentType,
        db_column="ScopeType",
        on_delete=models.DO_NOTHING,
        unique=False,
        null=True,
        default=get_location_content_type_id
    )

    scope_id = models.CharField(
        db_column="ScopeId",
        max_length=255,
        blank=True,
        null=True)

    class Meta:
        managed = True
        db_table = 'tblRelIndex'

    CARE_TYPE_OUT_PATIENT = "O"
    CARE_TYPE_IN_PATIENT = "I"
    CARE_TYPE_BOTH = "B"

    TYPE_MONTH = 12
    TYPE_QUARTER = 4
    TYPE_YEAR = 1


class RelativeDistribution(models.Model):
    CARE_TYPE_OUT_PATIENT = "O"
    CARE_TYPE_IN_PATIENT = "I"
    CARE_TYPE_BOTH = "B"

    TYPE_MONTH = 12
    TYPE_QUARTER = 4
    TYPE_YEAR = 1

    id = models.AutoField(db_column='DistrID', primary_key=True)
    product = models.ForeignKey(product_models.Product, models.DO_NOTHING, db_column='ProdID',
                                related_name="relative_distributions")
    type = models.SmallIntegerField(db_column='DistrType', choices=((TYPE_MONTH, gettext_lazy(
        "Month")), (TYPE_QUARTER, gettext_lazy("Quarter")), (TYPE_YEAR, gettext_lazy('Year'))))
    care_type = models.CharField(db_column='DistrCareType', max_length=1, choices=((CARE_TYPE_BOTH, gettext_lazy(
        "Both")), (CARE_TYPE_IN_PATIENT, gettext_lazy("In-Patient")), (CARE_TYPE_OUT_PATIENT, gettext_lazy("Out-Patient"))))
    period = models.SmallIntegerField(db_column='Period')
    percent = models.DecimalField(
        db_column='DistrPerc', max_digits=18, decimal_places=2, blank=True, null=True)

    validity_from = models.DateTimeField(db_column='ValidityFrom')
    validity_to = models.DateTimeField(
        db_column='ValidityTo', blank=True, null=True)
    legacy_id = models.IntegerField(
        db_column='LegacyID', blank=True, null=True)
    audit_user_id = models.IntegerField(db_column='AuditUserID')

    class Meta:
        managed = True
        db_table = 'tblRelDistr'

    CARE_TYPE_OUT_PATIENT = "O"
    CARE_TYPE_IN_PATIENT = "I"
    CARE_TYPE_BOTH = "B"

    TYPE_MONTH = 12
    TYPE_QUARTER = 4
    TYPE_YEAR = 1


class CapitationPayment(core_models.VersionedModel):
    id = models.AutoField(db_column='CapitationPaymentID', primary_key=True)
    uuid = models.CharField(db_column='CapitationPaymentUUID',
                            max_length=36, default=uuid.uuid4, unique=True)

    year = models.IntegerField('Year', null=False)
    month = models.IntegerField('Month', null=False)
    product = models.ForeignKey(Product, models.DO_NOTHING, db_column='ProductID',
                                related_name="capitation_payment_product")

    health_facility = models.ForeignKey(HealthFacility, models.DO_NOTHING, db_column='HfID',
                                        related_name="capitation_payment_health_facility")

    region_code = models.CharField(
        db_column='RegionCode', max_length=8, null=True, blank=True)
    region_name = models.CharField(
        db_column='RegionName', max_length=50, null=True, blank=True)

    district_code = models.CharField(
        db_column='DistrictCode', max_length=8, null=True, blank=True)
    district_name = models.CharField(
        db_column='DistrictName', max_length=50, null=True, blank=True)

    health_facility_code = models.CharField(db_column='HFCode', max_length=8)
    health_facility_name = models.CharField(db_column='HFName', max_length=100)

    acc_code = models.CharField(
        db_column='AccCode', max_length=25, null=True, blank=True)

    hf_level = models.CharField(
        db_column='HFLevel', max_length=100, blank=True, null=True)
    hf_sublevel = models.CharField(
        db_column='HFSublevel', max_length=100, blank=True, null=True)

    total_population = models.DecimalField(
        db_column='TotalPopulation', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    total_families = models.DecimalField(
        db_column='TotalFamilies', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    total_insured_insuree = models.DecimalField(
        db_column='TotalInsuredInsuree', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    total_insured_families = models.DecimalField(
        db_column='TotalInsuredFamilies', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    total_claims = models.DecimalField(
        db_column='TotalClaims', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    alc_contri_population = models.DecimalField(
        db_column='AlcContriPopulation', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    alc_contri_num_families = models.DecimalField(
        db_column='AlcContriNumFamilies', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    alc_contri_ins_population = models.DecimalField(
        db_column='AlcContriInsPopulation', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    alc_contri_ins_families = models.DecimalField(
        db_column='AlcContriInsFamilies', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    alc_contri_visits = models.DecimalField(
        db_column='AlcContriVisits', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    alc_contri_adjusted_amount = models.DecimalField(
        db_column='AlcContriAdjustedAmount', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    up_population = models.DecimalField(
        db_column='UPPopulation', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    up_num_families = models.DecimalField(
        db_column='UPNumFamilies', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    up_ins_population = models.DecimalField(
        db_column='UPInsPopulation', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    up_ins_families = models.DecimalField(
        db_column='UPInsFamilies', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    up_visits = models.DecimalField(
        db_column='UPVisits', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    up_adjusted_amount = models.DecimalField(
        db_column='UPAdjustedAmount', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    payment_cathment = models.DecimalField(
        db_column='PaymentCathment', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    total_adjusted = models.DecimalField(
        db_column='TotalAdjusted', max_digits=18, decimal_places=2, blank=True, null=True, default=0)

    class Meta:
        db_table = 'tblCapitationPayment'
