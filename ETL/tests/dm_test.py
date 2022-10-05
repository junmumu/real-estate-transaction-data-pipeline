import unittest
from datajob.datamart.ages_regist import OwnRegistAge
from datajob.datamart.sex_regist import OwnRegistSex
from datajob.datamart.own_regist_type import OwnRegistType
from datajob.datamart.monthly_apt_prc import MonthlyAptPrc


class MTest(unittest.TestCase):

    def test1(self):
        MonthlyAptPrc.save()
    
    def test2(self):
        OwnRegistType.save()

    def test3(self):
        OwnRegistSex.save()

    def test4(self):
        OwnRegistAge.save()



if __name__ == "__main__":
    """ This is executed when run from the command line """
    unittest.main()
