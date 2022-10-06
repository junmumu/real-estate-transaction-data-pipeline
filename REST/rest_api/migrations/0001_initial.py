# Generated by Django 4.1.1 on 2022-10-06 04:33

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='AccSellBuyAdrs',
            fields=[
                ('asba_idx', models.BigAutoField(primary_key=True, serialize=False)),
                ('regn', models.CharField(blank=True, max_length=30, null=True)),
                ('sell_tot', models.BigIntegerField(blank=True, null=True)),
                ('sell_rate', models.FloatField(blank=True, null=True)),
                ('buy_tot', models.BigIntegerField(blank=True, null=True)),
                ('buy_rate', models.FloatField(blank=True, null=True)),
            ],
            options={
                'db_table': 'acc_sell_buy_adrs',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='AccSellBuyAges',
            fields=[
                ('asba_idx', models.BigAutoField(primary_key=True, serialize=False)),
                ('ages', models.CharField(blank=True, max_length=30, null=True)),
                ('buy_tot', models.BigIntegerField(blank=True, null=True)),
                ('buy_rate', models.FloatField(blank=True, null=True)),
            ],
            options={
                'db_table': 'acc_sell_buy_ages',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='AccSellBuyAgesSido',
            fields=[
                ('asbas_idx', models.BigAutoField(primary_key=True, serialize=False)),
                ('ages', models.CharField(blank=True, max_length=30, null=True)),
                ('regn', models.CharField(blank=True, max_length=30, null=True)),
                ('buy_tot', models.BigIntegerField(blank=True, null=True)),
            ],
            options={
                'db_table': 'acc_sell_buy_ages_sido',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='AccSellBuyForeign',
            fields=[
                ('asbf_idx', models.BigAutoField(primary_key=True, serialize=False)),
                ('foreigner', models.CharField(blank=True, max_length=30, null=True)),
                ('buy_tot', models.BigIntegerField(blank=True, null=True)),
                ('buy_rate', models.FloatField(blank=True, null=True)),
            ],
            options={
                'db_table': 'acc_sell_buy_foreign',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='AccSellBuyForeignSido',
            fields=[
                ('asbfs_idx', models.BigAutoField(primary_key=True, serialize=False)),
                ('foreigner', models.CharField(blank=True, max_length=30, null=True)),
                ('regn', models.CharField(blank=True, max_length=30, null=True)),
                ('buy_tot', models.BigIntegerField(blank=True, null=True)),
            ],
            options={
                'db_table': 'acc_sell_buy_foreign_sido',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='AccSellBuySex',
            fields=[
                ('asbs_idx', models.BigAutoField(primary_key=True, serialize=False)),
                ('sex', models.CharField(blank=True, max_length=10, null=True)),
                ('buy_tot', models.BigIntegerField(blank=True, null=True)),
                ('buy_rate', models.FloatField(blank=True, null=True)),
            ],
            options={
                'db_table': 'acc_sell_buy_sex',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='AccSellBuySexSido',
            fields=[
                ('asbss_idx', models.BigAutoField(primary_key=True, serialize=False)),
                ('sex', models.CharField(blank=True, max_length=10, null=True)),
                ('regn', models.CharField(blank=True, max_length=30, null=True)),
                ('buy_tot', models.BigIntegerField(blank=True, null=True)),
            ],
            options={
                'db_table': 'acc_sell_buy_sex_sido',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='AccSellBuyType',
            fields=[
                ('asbt_idx', models.BigAutoField(primary_key=True, serialize=False)),
                ('cls', models.CharField(blank=True, max_length=30, null=True)),
                ('buy_tot', models.BigIntegerField(blank=True, null=True)),
                ('buy_rate', models.FloatField(blank=True, null=True)),
            ],
            options={
                'db_table': 'acc_sell_buy_type',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='AccSellBuyTypeSido',
            fields=[
                ('asbts_idx', models.BigAutoField(primary_key=True, serialize=False)),
                ('cls', models.CharField(blank=True, max_length=30, null=True)),
                ('regn', models.CharField(blank=True, max_length=30, null=True)),
                ('buy_tot', models.BigIntegerField(blank=True, null=True)),
            ],
            options={
                'db_table': 'acc_sell_buy_type_sido',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='AgesRegist',
            fields=[
                ('ar_idx', models.BigAutoField(primary_key=True, serialize=False)),
                ('ages', models.CharField(blank=True, max_length=30, null=True)),
                ('tot', models.BigIntegerField(blank=True, null=True)),
                ('rate', models.FloatField(blank=True, null=True)),
            ],
            options={
                'db_table': 'ages_regist',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='AuthGroup',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(blank=True, max_length=150, null=True, unique=True)),
            ],
            options={
                'db_table': 'auth_group',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='AuthGroupPermissions',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
            ],
            options={
                'db_table': 'auth_group_permissions',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='AuthPermission',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(blank=True, max_length=255, null=True)),
                ('codename', models.CharField(blank=True, max_length=100, null=True)),
            ],
            options={
                'db_table': 'auth_permission',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='AuthtokenToken',
            fields=[
                ('key', models.CharField(max_length=40, primary_key=True, serialize=False)),
                ('created', models.DateTimeField()),
            ],
            options={
                'db_table': 'authtoken_token',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='AuthUser',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('password', models.CharField(blank=True, max_length=128, null=True)),
                ('last_login', models.DateTimeField(blank=True, null=True)),
                ('is_superuser', models.BooleanField()),
                ('username', models.CharField(blank=True, max_length=150, null=True, unique=True)),
                ('first_name', models.CharField(blank=True, max_length=150, null=True)),
                ('last_name', models.CharField(blank=True, max_length=150, null=True)),
                ('email', models.CharField(blank=True, max_length=254, null=True)),
                ('is_staff', models.BooleanField()),
                ('is_active', models.BooleanField()),
                ('date_joined', models.DateTimeField()),
            ],
            options={
                'db_table': 'auth_user',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='AuthUserGroups',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
            ],
            options={
                'db_table': 'auth_user_groups',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='AuthUserUserPermissions',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
            ],
            options={
                'db_table': 'auth_user_user_permissions',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='DjangoAdminLog',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('action_time', models.DateTimeField()),
                ('object_id', models.TextField(blank=True, null=True)),
                ('object_repr', models.CharField(blank=True, max_length=200, null=True)),
                ('action_flag', models.IntegerField()),
                ('change_message', models.TextField(blank=True, null=True)),
            ],
            options={
                'db_table': 'django_admin_log',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='DjangoContentType',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('app_label', models.CharField(blank=True, max_length=100, null=True)),
                ('model', models.CharField(blank=True, max_length=100, null=True)),
            ],
            options={
                'db_table': 'django_content_type',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='DjangoMigrations',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('app', models.CharField(blank=True, max_length=255, null=True)),
                ('name', models.CharField(blank=True, max_length=255, null=True)),
                ('applied', models.DateTimeField()),
            ],
            options={
                'db_table': 'django_migrations',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='DjangoSession',
            fields=[
                ('session_key', models.CharField(max_length=40, primary_key=True, serialize=False)),
                ('session_data', models.TextField(blank=True, null=True)),
                ('expire_date', models.DateTimeField()),
            ],
            options={
                'db_table': 'django_session',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='MonthlyAptPrc',
            fields=[
                ('map_idx', models.BigAutoField(primary_key=True, serialize=False)),
                ('regn', models.CharField(blank=True, max_length=30, null=True)),
                ('date_ym', models.CharField(blank=True, max_length=10, null=True)),
                ('avg_price', models.BigIntegerField(blank=True, null=True)),
                ('avg_price_m2', models.BigIntegerField(blank=True, null=True)),
            ],
            options={
                'db_table': 'monthly_apt_prc',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='OwnRegistType',
            fields=[
                ('ort_idx', models.BigAutoField(primary_key=True, serialize=False)),
                ('cls', models.CharField(blank=True, max_length=30, null=True)),
                ('tot', models.BigIntegerField(blank=True, null=True)),
                ('rate', models.FloatField(blank=True, null=True)),
            ],
            options={
                'db_table': 'own_regist_type',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='SellBuyAgesYear',
            fields=[
                ('sbay_idx', models.BigAutoField(primary_key=True, serialize=False)),
                ('ages', models.CharField(blank=True, max_length=30, null=True)),
                ('year', models.CharField(blank=True, max_length=5, null=True)),
                ('buy_tot', models.BigIntegerField(blank=True, null=True)),
            ],
            options={
                'db_table': 'sell_buy_ages_year',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='SellBuyForeignYear',
            fields=[
                ('sbfy_idx', models.BigAutoField(primary_key=True, serialize=False)),
                ('foreigner', models.CharField(blank=True, max_length=30, null=True)),
                ('year', models.CharField(blank=True, max_length=5, null=True)),
                ('buy_tot', models.BigIntegerField(blank=True, null=True)),
            ],
            options={
                'db_table': 'sell_buy_foreign_year',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='SellBuySexYear',
            fields=[
                ('sbsy_idx', models.BigAutoField(primary_key=True, serialize=False)),
                ('sex', models.CharField(blank=True, max_length=10, null=True)),
                ('year', models.CharField(blank=True, max_length=5, null=True)),
                ('buy_tot', models.BigIntegerField(blank=True, null=True)),
            ],
            options={
                'db_table': 'sell_buy_sex_year',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='SellBuySudo',
            fields=[
                ('sbs_idx', models.BigAutoField(primary_key=True, serialize=False)),
                ('sudo', models.CharField(blank=True, max_length=5, null=True)),
                ('buy_tot', models.BigIntegerField(blank=True, null=True)),
                ('buy_rate', models.FloatField(blank=True, null=True)),
                ('sell_tot', models.BigIntegerField(blank=True, null=True)),
                ('sell_rate', models.FloatField(blank=True, null=True)),
            ],
            options={
                'db_table': 'sell_buy_sudo',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='SellBuySudoYear',
            fields=[
                ('sbsy_idx', models.BigAutoField(primary_key=True, serialize=False)),
                ('regn', models.CharField(blank=True, max_length=30, null=True)),
                ('year', models.CharField(blank=True, max_length=5, null=True)),
                ('sell_tot', models.BigIntegerField(blank=True, null=True)),
                ('buy_tot', models.BigIntegerField(blank=True, null=True)),
            ],
            options={
                'db_table': 'sell_buy_sudo_year',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='SellBuyTypeYear',
            fields=[
                ('sbty_idx', models.BigAutoField(primary_key=True, serialize=False)),
                ('cls', models.CharField(blank=True, max_length=30, null=True)),
                ('year', models.CharField(blank=True, max_length=5, null=True)),
                ('buy_tot', models.BigIntegerField(blank=True, null=True)),
            ],
            options={
                'db_table': 'sell_buy_type_year',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='SeoulAgesRegist',
            fields=[
                ('sar_idx', models.BigAutoField(primary_key=True, serialize=False)),
                ('ages', models.CharField(blank=True, max_length=30, null=True)),
                ('tot', models.BigIntegerField(blank=True, null=True)),
                ('rate', models.FloatField(blank=True, null=True)),
            ],
            options={
                'db_table': 'seoul_ages_regist',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='SeoulGuRegist',
            fields=[
                ('sgr_idx', models.BigAutoField(primary_key=True, serialize=False)),
                ('regn', models.CharField(blank=True, max_length=30, null=True)),
                ('tot', models.BigIntegerField(blank=True, null=True)),
                ('rate', models.FloatField(blank=True, null=True)),
            ],
            options={
                'db_table': 'seoul_gu_regist',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='SeoulOwnRegistType',
            fields=[
                ('sort_idx', models.BigAutoField(primary_key=True, serialize=False)),
                ('cls', models.CharField(blank=True, max_length=30, null=True)),
                ('tot', models.BigIntegerField(blank=True, null=True)),
                ('rate', models.FloatField(blank=True, null=True)),
            ],
            options={
                'db_table': 'seoul_own_regist_type',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='SeoulSexRegist',
            fields=[
                ('ssr_idx', models.BigAutoField(primary_key=True, serialize=False)),
                ('sex', models.CharField(blank=True, max_length=5, null=True)),
                ('tot', models.BigIntegerField(blank=True, null=True)),
                ('rate', models.FloatField(blank=True, null=True)),
            ],
            options={
                'db_table': 'seoul_sex_regist',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='SexRegist',
            fields=[
                ('sr_idx', models.BigAutoField(primary_key=True, serialize=False)),
                ('sex', models.CharField(blank=True, max_length=5, null=True)),
                ('tot', models.BigIntegerField(blank=True, null=True)),
                ('rate', models.FloatField(blank=True, null=True)),
            ],
            options={
                'db_table': 'sex_regist',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='SidoRegist',
            fields=[
                ('sr_idx', models.BigAutoField(primary_key=True, serialize=False)),
                ('regn', models.CharField(blank=True, max_length=30, null=True)),
                ('tot', models.BigIntegerField(blank=True, null=True)),
                ('rate', models.FloatField(blank=True, null=True)),
            ],
            options={
                'db_table': 'sido_regist',
                'managed': False,
            },
        ),
    ]
