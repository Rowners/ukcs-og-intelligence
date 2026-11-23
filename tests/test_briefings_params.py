import unittest

from src.intelligence.briefings import DEFAULT_MODEL, _allows_custom_temperature


class TemperaturePolicyTests(unittest.TestCase):
    def test_default_model_does_not_send_custom_temperature(self):
        self.assertEqual(DEFAULT_MODEL, "gpt-6.1-terra")
        self.assertFalse(_allows_custom_temperature(DEFAULT_MODEL))

    def test_gpt4_family_still_allows_temperature(self):
        self.assertTrue(_allows_custom_temperature("gpt-4o"))
        self.assertTrue(_allows_custom_temperature("gpt-4o-mini"))
        self.assertTrue(_allows_custom_temperature("gpt-4.1"))
        self.assertTrue(_allows_custom_temperature("gpt-4.1-mini"))

    def test_other_models_use_the_api_default(self):
        self.assertFalse(_allows_custom_temperature("gpt-6.1-terra"))
        self.assertFalse(_allows_custom_temperature("gpt-5.4"))


if __name__ == "__main__":
    unittest.main()
