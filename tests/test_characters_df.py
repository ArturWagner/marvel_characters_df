import unittest
from marvel_characters_df import extract_dataframe


class TestCharactersDF(unittest.TestCase):
    def setUp(self) -> None:
        self.final_df = extract_dataframe()

    def test_df_columns(self) -> None:
        '''
            Testing if dataframe have the desired columns
        '''
        self.assertEqual(
            [
                "id",
                "name",
                "description",
                "comics",
                "series",
                "stories",
                "events"
            ],
            self.final_df.columns.values.tolist(),
            'Final df columns not equal expected columns'
        )

    def test_df_row_numbers(self) -> None:
        '''
            Testing if the df has more rows than when this
            test was defined
        '''

        self.assertGreater(
            len(self.final_df),
            1500,
            'Request row number less than expect'
        )


if __name__ == '__main__':
    unittest.main()
