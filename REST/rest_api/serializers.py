from rest_framework import serializers
from rest_api.models import *


class MonthlyAptPrcSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = MonthlyAptPrc
        fields = ['regn', 'date_ym', 'avg_price', 'avg_price_m2']

class SidoRegistSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = SidoRegist
        fields = ['regn', 'tot', 'rate']

class SeoulGuRegistSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = SeoulGuRegist
        fields = ['regn', 'tot', 'rate']


# class CoFacilitySerializer(serializers.HyperlinkedModelSerializer):
#     class Meta:
#         model = CoFacility
#         fields = ['loc', 'fac_popu', 'qur_rate', 'std_day']


# class CoPopuDensitySerializer(serializers.HyperlinkedModelSerializer):
#     class Meta:
#         model = CoPopuDensity
#         fields = ['loc', 'popu_density', 'qur_rate', 'std_day']


# class CoVaccineSerializer(serializers.HyperlinkedModelSerializer):
#     class Meta:
#         model = CoVaccine
#         fields = ['loc', 'v1th_rate', 'v2th_rate', 'v3th_rate', 'v4th_rate', 'qur_rate', 'std_day']


# class CoWeekdaySerializer(serializers.HyperlinkedModelSerializer):
#     class Meta:
#         model = CoWeekday
#         fields = ['sun', 'mon', 'tue', 'wed', 'thu', 'fri','sat','std_day']
