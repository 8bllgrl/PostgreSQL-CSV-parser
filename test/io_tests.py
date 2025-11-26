import unittest
from pathlib import Path

from csv_structure_provider import list_csv_files_in_directory, list_quest_files_for_language, Config


class TestIO(unittest.TestCase):

    def test_files_exist(self):
        file_path = Path(r'C:\Users\sbelknap\PycharmProjects\dialogdbconn\rsrc\csv')
        self.assertTrue(file_path.exists(), f"File {file_path} does not exist")

        project_root = Path(__file__).resolve().parent.parent  # Adjust if necessary for deeper nested directories
        file_path = project_root / 'rsrc' / 'csv'   # Relative to the project root
        self.assertTrue(file_path.exists(), f"File {file_path} does not exist")

        file_path = Path(r'C:\Users\sbelknap\PycharmProjects\dialogdbconn\rsrc\csv')
        self.assertTrue(file_path.exists(), f"File {file_path} does not exist")

        project_root = Path(__file__).resolve().parent.parent  # Adjust if necessary for deeper nested directories
        file_path = project_root / 'rsrc' / 'csv' / 'eng' / 'cut_scene'  # Relative to the project root
        self.assertTrue(file_path.exists(), f"File {file_path} does not exist")

        project_root = Path(__file__).resolve().parent.parent  # Adjust if necessary for deeper nested directories
        file_path = project_root / 'rsrc' / 'csv' / 'eng' / 'cut_scene' / '022'  # Relative to the project root
        self.assertTrue(file_path.exists(), f"File {file_path} does not exist")

        project_root = Path(__file__).resolve().parent.parent
        file_path = project_root / 'rsrc' / 'csv' / 'eng' / 'Quest.csv'
        self.assertTrue(file_path.exists(), f"File {file_path} does not exist")

        project_root = Path(__file__).resolve().parent.parent
        file_path = project_root / 'rsrc' / 'csv' / 'jp' / 'Quest.csv'
        self.assertTrue(file_path.exists(), f"File {file_path} does not exist")




    def test_config_init(self):
        # base_dir = r'C:\Users\sbelknap\PycharmProjects\dialogdbconn\rsrc\csv'
        # expected_JP = r'C:\Users\sbelknap\PycharmProjects\dialogdbconn\rsrc\csv\jp'
        # expected_ENG = r'C:\Users\sbelknap\PycharmProjects\dialogdbconn\rsrc\csv\eng'
        #
        project_root = Path(__file__).resolve().parent.parent  # Adjust to project root directory
        base_dir = project_root / 'rsrc' / 'csv'
        expected_JP = base_dir / 'jp'
        expected_ENG = base_dir / 'eng'

        # first none
        self.assertEqual(Config.BASE_CSV_DIR, None)

        # then initialized.
        Config.initialize_language_base_directories(base_dir)
        # the directories are set correctly after initialization
        self.assertEqual(str(Config.BASE_CSV_DIR), str(base_dir))
        self.assertEqual(str(Config.JP_DIR), str(expected_JP))
        self.assertEqual(str(Config.ENG_DIR), str(expected_ENG))

    def test_file_in(self):
        project_root = Path(__file__).resolve().parent.parent
        base_dir = project_root / 'rsrc' / 'csv'

        expected_files = [
            str(base_dir / 'eng' / 'quest' / '000' / 'ClsArc000_00021.csv'),
            str(base_dir / 'jp' / 'quest' / '000' / 'ClsArc000_00021.csv'),
            str(base_dir / 'eng' / 'cut_scene' / '022' / 'VoiceMan_02200.csv'),
            str(base_dir / 'jp' / 'cut_scene' / '022' / 'VoiceMan_02200.csv')
        ]
        result = list_csv_files_in_directory(str(project_root / 'rsrc'))
        for expected_file in expected_files:
            self.assertIn(expected_file, result)

    def test_differentiate_eng_jp_quest_files(self):
        project_root = Path(__file__).resolve().parent.parent
        base_dir = project_root / 'rsrc' / 'csv'

        expected_jp_file = str(base_dir / 'jp' / 'quest' / '000' / 'ClsArc000_00021.csv')
        expected_eng_file = str(base_dir / 'eng' / 'quest' / '000' / 'ClsArc000_00021.csv')
        un_expected_file = str(base_dir / 'jp' / 'BGM.csv')

        self.assertIn(expected_jp_file,
                      list_quest_files_for_language('jp', str(base_dir)))
        self.assertIn(expected_eng_file,
                      list_quest_files_for_language('eng', str(base_dir)))
        self.assertNotIn(un_expected_file, list_quest_files_for_language('eng', str(base_dir)))
        self.assertNotIn(un_expected_file, list_quest_files_for_language('jp', str(base_dir)))

    @staticmethod
    def run_all_tests():
        # Load all the test cases from the TestDatabase class
        test_loader = unittest.TestLoader()
        test_suite = test_loader.loadTestsFromTestCase(TestIO)

        # Create a test runner and run the tests
        test_runner = unittest.TextTestRunner()
        result = test_runner.run(test_suite)
        return result