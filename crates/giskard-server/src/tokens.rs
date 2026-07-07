//! Token dashboard assembly & cost estimation (spec §10.2/§10.4).
//!
//! Turns a persisted [`DailyTokenLedger`] into a [`TokenReport`] with day/week/month/total windows
//! derived on read (single source of truth = `by_day`), plus an optional euro cost estimate.

use chrono::Utc;

use giskard_core::token::{ByModel, DailyTokenLedger, iso_week_of};
use giskard_persist::config::TokensConfig;
use giskard_proto::TokenReport;

/// Build the dashboard report for a ledger, deriving the current day/week/month windows from
/// today's date and attaching a cost estimate when enabled (§10.4).
pub fn build_report(ledger: &DailyTokenLedger, cfg: &TokensConfig) -> TokenReport {
    let today = Utc::now().format("%Y-%m-%d").to_string();
    let month = today[..7].to_string();
    let week = iso_week_of(&today);

    let estimated_cost_eur = if cfg.cost_estimation {
        Some(estimate_cost(&ledger.by_model, cfg))
    } else {
        None
    };

    TokenReport {
        total: ledger.total,
        today: ledger.day_total(&today),
        this_week: week
            .as_deref()
            .map(|w| ledger.iso_week_total(w))
            .unwrap_or_default(),
        this_month: ledger.month_total(&month),
        by_day: ledger.by_day.clone(),
        by_model: ledger.by_model.clone(),
        estimated_cost_eur,
    }
}

/// Estimate spend in euros from the per-model usage and the configured `[tokens.rates]` table
/// (§10.4). Models without a configured rate contribute nothing.
pub fn estimate_cost(by_model: &ByModel, cfg: &TokensConfig) -> f64 {
    let mut total = 0.0;
    for (provider, models) in &by_model.0 {
        for (model, usage) in models {
            let key = format!("{provider}/{model}");
            if let Some(rate) = cfg.rates.get(&key) {
                total += (usage.input as f64 / 1_000_000.0) * rate.input_per_mtok_eur;
                total += (usage.output as f64 / 1_000_000.0) * rate.output_per_mtok_eur;
            }
        }
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;
    use giskard_core::token::TokenUsage;
    use giskard_persist::config::ModelRate;

    #[test]
    fn cost_estimation_sums_configured_rates() {
        let mut by_model = ByModel::default();
        by_model.record("openai", "gpt-5.5", &TokenUsage::new(2_000_000, 1_000_000));
        by_model.record("acme", "unpriced", &TokenUsage::new(5_000_000, 5_000_000));

        let mut cfg = TokensConfig {
            cost_estimation: true,
            ..Default::default()
        };
        cfg.rates.insert(
            "openai/gpt-5.5".into(),
            ModelRate {
                input_per_mtok_eur: 3.0,
                output_per_mtok_eur: 10.0,
            },
        );

        // 2M input * €3 + 1M output * €10 = €16; the unpriced model adds nothing.
        let cost = estimate_cost(&by_model, &cfg);
        assert!((cost - 16.0).abs() < 1e-9, "got {cost}");
    }

    #[test]
    fn report_windows_and_cost_gate() {
        let mut ledger = DailyTokenLedger::default();
        let today = Utc::now().format("%Y-%m-%d").to_string();
        ledger.record(&today, "openai", "gpt-5.5", &TokenUsage::new(100, 40));

        // Cost off by default.
        let report = build_report(&ledger, &TokensConfig::default());
        assert_eq!(report.total.total, 140);
        assert_eq!(report.today.total, 140);
        assert_eq!(report.this_month.total, 140);
        assert!(report.estimated_cost_eur.is_none());

        // Cost on with an empty rate table ⇒ Some(0.0).
        let report = build_report(
            &ledger,
            &TokensConfig {
                cost_estimation: true,
                ..Default::default()
            },
        );
        assert_eq!(report.estimated_cost_eur, Some(0.0));
    }
}
