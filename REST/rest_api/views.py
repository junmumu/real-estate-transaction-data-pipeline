from datetime import datetime, timedelta
from rest_framework import viewsets, permissions
from rest_api.models import *
from rest_api.serializers import *
from django.http import JsonResponse
from drf_yasg.utils import swagger_auto_schema
from drf_yasg.openapi import Parameter, Schema, IN_QUERY, TYPE_STRING, TYPE_OBJECT, TYPE_INTEGER, TYPE_NUMBER
from django.core import serializers



def get_queryset_by_date(model, query_params):
    if 'location' not in query_params:
        loc = '서울특별시'
    else:
        loc = query_params['location']

    if ~('start_date' in query_params) and ~('end_date' in query_params):
        queryset = model.objects.filter(regn=loc) \
                                .order_by('-date_ym')
        return queryset

    if 'start_date' in query_params and 'end_date' in query_params:
        start_date = query_params['start_date']
        end_date = query_params['end_date']
        queryset = model.objects.filter(regn=loc) \
                                .filter(date_ym__range=(start_date, end_date)) \
                                .order_by('-date_ym')
        return queryset

    if 'start_date' in query_params:
        start_date = query_params['start_date']
        queryset = model.objects.filter(regn=loc) \
                                .filter(date_ym__gt=start_date) \
                                .order_by('-date_ym')
        return queryset

    if 'end_date' in query_params:
        end_date = query_params['end_date']
        queryset = model.objects.filter(regn=loc) \
                                .filter(date_ym__lt=end_date) \
                                .order_by('-date_ym')
        return queryset

    return queryset


class MonthlyAptPrcViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = MonthlyAptPrc.objects.all()
    serializer_class = MonthlyAptPrcSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary="월간 아파트 매매 실거래 가격",
        operation_description="""지역이름 생략 시 서울특별시 데이터를 반환합니다. <br>
            시작년월와 끝년월을 모두 생략하면 최근 1년 데이터를 반환합니다. <br>
            시작년월만 입력하면 시작날짜부터 저번달까지의 데이터를 반환합니다.<br>
            끝년월만 입력하면 끝날짜 이전 데이터를 반환합니다.<br> """,
        manual_parameters=[
            Parameter("location", IN_QUERY, type=TYPE_STRING,
                      description="지역이름(광역시도) ex) 서울특별시, 경기도, 경상남도, 제주특별자치도, 세종특별자치시 ...", required=False),
            Parameter("start_date", IN_QUERY, type=TYPE_STRING,
                      description="시작년월, (format : yyyy-MM)", required=False),
            Parameter("end_date", IN_QUERY, type=TYPE_STRING,
                      description="끝년월, (format : yyyy-MM)", required=False),
        ],
    )
    def list(self, request):
        query_parmas = request.query_params
        queryset = get_queryset_by_date(MonthlyAptPrc, query_parmas)
        print(query_parmas)
        #serialized = serializers.serialize('json', queryset)
        #queryset = MonthlyAptPrc.objects.filter(regn=query_parmas['location']).order_by('-date_ym')
        serializer = self.get_serializer(queryset, many=True)
        return JsonResponse(serializer.data, safe=False)

    @swagger_auto_schema(auto_schema=None)
    def retrieve(self, request):
        pass


class SidoRegistViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = SidoRegist.objects.all()
    serializer_class = SidoRegistSerializer
    permission_classes = [permissions.IsAuthenticated]

    @swagger_auto_schema(
        operation_summary="전국 17개 광역시도별 등기 구분 수",
        operation_description="""지역이름 생략 시 서울특별시 데이터를 반환합니다. """,
        manual_parameters=[
            Parameter("location", IN_QUERY, type=TYPE_STRING,
                      description="지역이름(광역시도) ex) 서울특별시, 경기도, 경상남도, 제주특별자치도, 세종특별자치시 ...", required=False)
        ],
        responses={
            200: Schema(
                'SidoRegist',
                type = TYPE_OBJECT,
                properties={
                    'regn': Schema('광역시도명', type=TYPE_STRING),
                    'tot': Schema('등기수', type=TYPE_INTEGER),
                    'rate': Schema('비율', type=TYPE_NUMBER)
                }
            )
        }
    )
    def list(self, request):
        query_params = request.query_params
        if 'location' not in query_params:
            loc = '서울특별시'
        else:
            loc = query_params['location']
        queryset = SidoRegist.objects.filter(regn=loc)
        serializer = self.get_serializer(queryset, many=True)
        return JsonResponse(serializer.data, safe=False)

    @swagger_auto_schema(auto_schema=None)
    def retrieve(self, request):
        pass


# # 2022-09-23 조건문안에 return문 안내해야함!!!!!!!!!!!!!!!!!!!
# def get_queryset_by_date(model, query_params):
#     if ~('start_date' in query_params) and ~('end_date' in query_params):
#         queryset = model.objects.filter(std_day__gt=(datetime.today() - timedelta(7)))
#         return queryset
#     if 'start_date' in query_params and 'end_date' in query_params:
#         start_date = query_params['start_date']
#         end_date = query_params['end_date']
#         queryset = model.objects.filter(std_day__gt=start_date).filter(std_day__lt=end_date)
#         return queryset

#     if 'start_date' in query_params:
#         start_date = query_params['start_date']
#         queryset = model.objects.filter(std_day__gt=start_date)
#         return queryset

#     if 'end_date' in query_params:
#         end_date = query_params['end_date']
#         queryset = model.objects.filter(std_day__lt=end_date)
#         return queryset

#     return queryset

# class CoFacilityViewSet(viewsets.ReadOnlyModelViewSet):
#     queryset = CoFacility.objects.all().order_by('-std_day')
#     serializer_class = CoFacilitySerializer
#     permission_classes = [permissions.IsAuthenticated]

#     @swagger_auto_schema(
#         operation_summary="10만명당 다중이용시설의 개수와 10만명 당 코로나 발생자 수",
#         operation_description="""시작날짜와 끝날짜를 모두 생략하면 최근 1주일 데이터를 반환합니다. <br>
#             시작날짜만 입력하면 시작날짜 이후의 데이터를 반환합니다.<br>
#             끝날짜만 입력하면 끝날짜 이전 데이터를 반환합니다.<br> """,
#         manual_parameters=[
#             Parameter("start_date", IN_QUERY, type=TYPE_STRING,
#                       description="시작 날짜, (format :yyyy-MM-dd)", required=True),
#             Parameter("end_date", IN_QUERY, type=TYPE_STRING,
#                       description="끝날짜, (format :yyyy-MM-dd)", required=False),
#         ],
#     )
#     def list(self, request):
#         query_parmas = request.query_params
#         queryset = get_queryset_by_date(CoFacility, query_parmas)
#         print(query_parmas)
#         serializer = self.get_serializer(queryset, many=True)
#         return JsonResponse(serializer.data)

#     @swagger_auto_schema(auto_schema=None)
#     def retrieve(self, request):
#         pass

# class CoPopuDensityViewSet(viewsets.ReadOnlyModelViewSet):
#     queryset = CoPopuDensity.objects.all().order_by('-std_day')
#     serializer_class = CoPopuDensitySerializer
#     permission_classes = [permissions.IsAuthenticated]

#     @swagger_auto_schema(
#         operation_summary="인구밀도와 10만명당 코로나 발생 환자 간의 상관관계",
#         operation_description="""시작날짜와 끝날짜를 모두 생략하면 최근 1주일 데이터를 반환합니다. <br>
#         시작날짜만 입력하면 시작날짜 이후의 데이터를 반환합니다.<br>
#         끝날짜만 입력하면 끝날짜 이전 데이터를 반환합니다.<br>
#          """,
#         manual_parameters=[
#             Parameter("start_date", IN_QUERY, type=TYPE_STRING,
#                       description="시작 날짜, (format :yyyy-MM-dd), (required : False)"),
#             Parameter("end_date", IN_QUERY, type=TYPE_STRING,
#                       description="끝날짜, (format :yyyy-MM-dd), (required : False)", required=False),
#         ],
#     )

#     def list(self, request):
#         query_parmas = request.query_params
#         queryset = get_queryset_by_date(CoPopuDensity, query_parmas)
#         serializer = self.get_serializer(queryset, many=True)
#         return JsonResponse(serializer.data, safe=False)


# class CoVaccineViewSet(viewsets.ReadOnlyModelViewSet):
#     queryset = CoVaccine.objects.all().order_by('-std_day')
#     serializer_class = CoVaccineSerializer
#     permission_classes = [permissions.IsAuthenticated]

# class CoWeekdayViewSet(viewsets.ReadOnlyModelViewSet):
#     queryset = CoWeekday.objects.all().order_by('-std_day')
#     serializer_class = CoWeekdaySerializer
#     permission_classes = [permissions.IsAuthenticated]
    