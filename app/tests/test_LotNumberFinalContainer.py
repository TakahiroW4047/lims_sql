import pandas as pd
import unittest

from lims_sql import LotNumberFinalContainer

class LotNumberFinalContainerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        month_count = 3
        cls.instance = LotNumberFinalContainer(cutoff_month_count=month_count)
        cls.df = pd.DataFrame(
            {
                'LOT_NUMBER': [
                    'TVA20001', 'TVA20001A',
                    'TAA20001A','TAA20001',
                    'THA20001', 'THA20001A',
                    'TRA20001', 'TRA20001A',
                    'TNA20001', 'TNA20001A',
                    'NA', 'NA', 'NA', 'NA', 'NA'
                    ],
                'LOT_ID': [
                    1, 2, 
                    3, 4,
                    5, 6,
                    7, 8,
                    9, 10,
                    11, 12, 13, 14, 15
                    ],
                'MATERIAL_NAME': [
                    'RVWF', 'RVWF', 
                    'NA', 'NA',
                    'AHF-M BULK', 'AHFM FINAL CONTAINER',
                    'RAHF BDS', 'RAHF FINAL CONTAINER',
                    'RFIX_BDS', 'RFIX_FINAL_CONTAINER',
                    'RAHF BDS', 'RAHF BDS', 'RAHF BDS', 'RAHF_PFM_BDS', 'BAX 855'
                    ],
                'MATERIAL_TYPE': [
                    'BULK DRUG', 'FINAL CONTAINER',
                    'NA', 'NA',
                    'FORM_FINISH', 'FINAL_CONTAINER',
                    'FORM_FINISH', 'FINAL_CONTAINER',
                    'FORM_FINISH', 'FINAL_CONTAINER',
                    'CELL_CULTURE', 'CELL_CULTURE1', 'PURIFICATION', 'NA', 'NA'
                    ]
            }
        )

    def test_filter_vonvendi(self):
        result_actual = self.instance._filter_vonvendi(self.df)['LOT_ID'].values
        result_expected = [1,2]
        self.assertCountEqual(result_actual, result_expected)

    def test_filter_advate(self):
        result_actual = self.instance._filter_advate(self.df)['LOT_ID'].values
        result_expected = [3,4]
        self.assertCountEqual(result_actual, result_expected)

    def test_filter_hemofil(self):
        result_actual = self.instance._filter_hemofil(self.df)['LOT_ID'].values
        result_expected = [5, 6]
        self.assertCountEqual(result_actual, result_expected)

    def test_filter_recombinant(self):
        result_actual = self.instance._filter_recombinant(self.df)['LOT_ID'].values
        result_expected = [7, 8]
        self.assertCountEqual(result_actual, result_expected)

    def test_filter_rixubis(self):
        result_actual = self.instance._filter_rixubis(self.df)['LOT_ID'].values
        result_expected = [9, 10]
        self.assertCountEqual(result_actual, result_expected)

    def test_filter_bds(self):
        result_actual = self.instance._filter_bds(self.df)['LOT_ID'].values
        result_expected = [11, 12, 13, 14, 15]
        self.assertCountEqual(result_actual, result_expected)

if __name__ == "__main__":
    unittest.main()