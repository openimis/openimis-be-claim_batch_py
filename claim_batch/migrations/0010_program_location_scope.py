from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("claim_batch", "0009_remove_batchrun_legacy_id_remove_batchrun_location_and_more"),
        ("program", "0004_program_code"),
        ("location", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="ProgramLocationScope",
            fields=[
                ("id", models.AutoField(primary_key=True, serialize=False)),
                ("scope_key", models.CharField(max_length=64, unique=True)),
                (
                    "location",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.DO_NOTHING,
                        to="location.location",
                    ),
                ),
                (
                    "program",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.DO_NOTHING,
                        to="program.program",
                    ),
                ),
            ],
            options={"db_table": "tblClaimBatchProgramLocationScope"},
        ),
    ]
