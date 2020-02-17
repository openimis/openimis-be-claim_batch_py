from core import fields
from core import models as core_models
from django.db import models
from location import models as location_models
from product import models as product_models


class BatchRun(models.Model):
    id = models.AutoField(db_column='RunID', primary_key=True)
    legacy_id = models.IntegerField(
        db_column='LegacyID', blank=True, null=True)
    location = models.ForeignKey(
        location_models.Location, models.DO_NOTHING,
        db_column='LocationId', blank=True, null=True)
    run_date = fields.DateTimeField(db_column='RunDate')
    validity_from = fields.DateTimeField(db_column='ValidityFrom')
    validity_to = fields.DateTimeField(
        db_column='ValidityTo', blank=True, null=True)

    audit_user_id = models.IntegerField(db_column='AuditUserID')
    run_year = models.IntegerField(db_column='RunYear')
    run_month = models.SmallIntegerField(db_column='RunMonth')

    class Meta:
        managed = False
        db_table = 'tblBatchRun'


class RelativeIndex(models.Model):
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
    validity_from = models.DateTimeField(db_column='ValidityFrom')
    validity_to = models.DateTimeField(
        db_column='ValidityTo', blank=True, null=True)
    legacy_id = models.IntegerField(
        db_column='LegacyID', blank=True, null=True)
    audit_user_id = models.IntegerField(db_column='AuditUserID')
    location = models.ForeignKey(
        location_models.Location, models.DO_NOTHING, db_column='LocationId', blank=True, null=True,
        related_name="relative_indexes"
    )

    class Meta:
        managed = False
        db_table = 'tblRelIndex'

    CARE_TYPE_OUT_PATIENT = "O"
    CARE_TYPE_IN_PATIENT = "I"
    CARE_TYPE_BOTH = "B"

    TYPE_MONTH = 12
    TYPE_QUARTER = 4
    TYPE_YEAR = 1


class RelativeDistribution(models.Model):
    id = models.AutoField(db_column='DistrID', primary_key=True)
    product = models.ForeignKey(product_models.Product, models.DO_NOTHING, db_column='ProdID',
                                related_name="relative_distributions")
    type = models.SmallIntegerField(db_column='DistrType')
    care_type = models.CharField(db_column='DistrCareType', max_length=1)
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
        managed = False
        db_table = 'tblRelDistr'

    CARE_TYPE_OUT_PATIENT = "O"
    CARE_TYPE_IN_PATIENT = "I"
    CARE_TYPE_BOTH = "B"

    TYPE_MONTH = 12
    TYPE_QUARTER = 4
    TYPE_YEAR = 1

