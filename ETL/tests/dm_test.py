import unittest
from datajob.datamart.acc_sell_buy_adrs import AccSellBuyAdrs
from datajob.datamart.ages_regist import OwnRegistAge
from datajob.datamart.sex_regist import OwnRegistSex
from datajob.datamart.own_regist_type import OwnRegistType
from datajob.datamart.monthly_apt_prc import MonthlyAptPrc
from datajob.datamart.sell_buy_sudo import SellBuySudo
from datajob.datamart.sell_buy_sudo_year import SellBuySudoYear
from datajob.datamart.seoul_gu_regist import SeoulGuRegist
from datajob.datamart.sido_regist import SidoRegist


class MTest(unittest.TestCase):

    def test1(self):
        MonthlyAptPrc.save()
    
    def test2(self):
        OwnRegistType.save()

    def test3(self):
        OwnRegistSex.save()

    def test4(self):
        OwnRegistAge.save()

    def test2(self):
        AccSellBuyAdrs.save()

    def test3(self):
        SellBuySudo.save()

    def test4(self):
        SellBuySudoYear.save()

    def test5(self):
        SeoulGuRegist.save()

    def test6(self):
        SidoRegist.save()



if __name__ == "__main__":
    """ This is executed when run from the command line """
    unittest.main()
