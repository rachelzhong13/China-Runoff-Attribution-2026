import unittest

import numpy as np

from recompute_model_events import sensitivity_fieldnames
from sri_core import (
    backward_running_mean,
    corrected_frequency_ratio,
    corrected_rate_ratio,
    count_drought_events,
    count_flood_events,
    fit_monthly_calibration,
    fit_monthly_sri,
    transform_monthly_sri,
    undefined_zero_frequency_ratio,
)


class SriCoreTests(unittest.TestCase):
    def test_backward_running_mean_requires_complete_window(self):
        values = np.array([1.0, 2.0, 3.0, 4.0, np.nan, 6.0])
        result = backward_running_mean(values, window=3)
        np.testing.assert_allclose(result[:2], [np.nan, np.nan], equal_nan=True)
        self.assertEqual(result[2], 2.0)
        self.assertEqual(result[3], 3.0)
        self.assertTrue(np.isnan(result[4]))
        self.assertTrue(np.isnan(result[5]))

    def test_monthly_sri_is_finite_for_valid_nonnegative_series(self):
        rng = np.random.default_rng(12)
        years = np.repeat(np.arange(1950, 2020), 12 * 31)
        months = np.tile(np.repeat(np.arange(1, 13), 31), 70)
        values = rng.gamma(shape=2.0, scale=3.0, size=len(years))
        values[rng.random(len(values)) < 0.03] = 0.0
        result = fit_monthly_sri(values, years, months)
        self.assertEqual(result.failed_months, 0)
        self.assertEqual(result.negative_input_count, 0)
        self.assertTrue(np.all(np.isfinite(result.values)))
        self.assertEqual(len(result.selected_distributions), 12)

    def test_monthly_sri_is_invariant_to_positive_scale(self):
        rng = np.random.default_rng(19)
        years = np.repeat(np.arange(1950, 2020), 12 * 31)
        months = np.tile(np.repeat(np.arange(1, 13), 31), 70)
        values = rng.gamma(shape=1.7, scale=2.5, size=len(years))
        values[rng.random(len(values)) < 0.02] = 0.0
        base = fit_monthly_sri(values, years, months)
        scaled = fit_monthly_sri(values * 1e120, years, months)
        self.assertEqual(base.selected_distributions, scaled.selected_distributions)
        np.testing.assert_allclose(base.values, scaled.values, rtol=1e-10, atol=1e-10)

    def test_separate_fit_and_transform_matches_convenience_function(self):
        rng = np.random.default_rng(23)
        years = np.repeat(np.arange(1950, 2020), 12 * 31)
        months = np.tile(np.repeat(np.arange(1, 13), 31), 70)
        values = rng.gamma(shape=2.1, scale=1.8, size=len(years))
        values[rng.random(len(values)) < 0.04] = 0.0
        calibration = fit_monthly_calibration(values, years, months)
        separate = transform_monthly_sri(values, months, calibration)
        combined = fit_monthly_sri(values, years, months)
        self.assertEqual(separate.selected_distributions, combined.selected_distributions)
        np.testing.assert_allclose(separate.values, combined.values, rtol=0.0, atol=0.0)

    def test_common_calibration_preserves_reference_but_shifts_target(self):
        rng = np.random.default_rng(29)
        years = np.repeat(np.arange(1950, 2020), 12 * 31)
        months = np.tile(np.repeat(np.arange(1, 13), 31), 70)
        reference = rng.gamma(shape=2.0, scale=2.0, size=len(years))
        calibration = fit_monthly_calibration(reference, years, months)
        reference_sri = transform_monthly_sri(reference, months, calibration)
        target_sri = transform_monthly_sri(reference * 1.5, months, calibration)
        self.assertGreater(np.nanmean(target_sri.values), np.nanmean(reference_sri.values))

    def test_event_definitions(self):
        drought = np.concatenate(
            [np.zeros(3), np.full(19, -0.6), np.zeros(2), np.full(20, -0.6)]
        )
        self.assertEqual(count_drought_events(drought), 1)

        flood = np.concatenate(
            [np.array([0.6]), np.zeros(19), np.array([0.7]), np.zeros(20), np.array([0.8])]
        )
        self.assertEqual(count_flood_events(flood), 2)

        separated = np.concatenate([np.array([0.6]), np.zeros(15), np.array([0.7])])
        self.assertEqual(count_flood_events(separated, reset_gap=10), 2)
        self.assertEqual(count_flood_events(separated, reset_gap=20), 1)

    def test_full_sensitivity_field_grid(self):
        fields = sensitivity_fieldnames()
        self.assertEqual(len(fields), 24)
        self.assertIn("Flood_FR_T05_G10", fields)
        self.assertIn("Flood_FR_T10_G30", fields)

    def test_frequency_ratio_rules(self):
        self.assertEqual(corrected_frequency_ratio(0, 0), 1.0)
        self.assertEqual(corrected_frequency_ratio(1, 0), 3.0)
        self.assertAlmostEqual(corrected_rate_ratio(10, 10, 40, 30), 0.75)
        self.assertEqual(undefined_zero_frequency_ratio(4, 2), 2.0)
        self.assertTrue(np.isnan(undefined_zero_frequency_ratio(0, 0)))
        self.assertTrue(np.isnan(undefined_zero_frequency_ratio(1, 0)))


if __name__ == "__main__":
    unittest.main()




