import unittest

from datajob.etl.transform.local_code import LocalCodeTransformer

class MTest(unittest.TestCase):

    def test1(self):
        LocalCodeTransformer.transform()



if __name__ == "__main__":
    unittest.main()