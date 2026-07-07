use serde::{Deserialize, Serialize};

/// Token usage reported on turn completion (spec §4.5).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenUsage {
    pub input: u64,
    pub output: u64,
    pub total: u64,
}

impl TokenUsage {
    pub fn new(input: u64, output: u64) -> Self {
        Self {
            input,
            output,
            total: input + output,
        }
    }

    /// Add another usage into this one (in-place).
    pub fn add(&mut self, other: &Self) {
        self.input += other.input;
        self.output += other.output;
        self.total += other.total;
    }

    /// Compute the context-gauge ratio: used / context_window (spec §10.3).
    ///
    /// Returns a fraction in [0.0, ∞). Values > 1.0 mean the usage exceeds the
    /// model's context window (which may happen if the gauge uses cumulative
    /// usage as a proxy).
    pub fn context_ratio(&self, context_window: u32) -> f64 {
        if context_window == 0 {
            return 0.0;
        }
        self.total as f64 / context_window as f64
    }
}

/// Aggregated token usage with per-model breakdown (spec §5.3 / §10.2).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenLedger {
    pub total: TokenUsage,
    #[serde(default)]
    pub by_model: std::collections::BTreeMap<String, TokenUsage>,
}

impl TokenLedger {
    pub fn record(&mut self, model_key: &str, usage: &TokenUsage) {
        self.total.add(usage);
        self.by_model
            .entry(model_key.to_string())
            .or_default()
            .add(usage);
    }
}

/// Per-day token bucket (spec §5.3).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DailyTokenLedger {
    pub total: TokenUsage,
    #[serde(default)]
    pub by_day: std::collections::BTreeMap<String, TokenUsage>,
    #[serde(default)]
    pub by_model: std::collections::BTreeMap<String, TokenUsage>,
}

impl DailyTokenLedger {
    pub fn record(&mut self, date: &str, model_key: &str, usage: &TokenUsage) {
        self.total.add(usage);
        self.by_day.entry(date.to_string()).or_default().add(usage);
        self.by_model
            .entry(model_key.to_string())
            .or_default()
            .add(usage);
    }

    /// Derive weekly totals from `by_day` buckets (spec §10.2).
    ///
    /// `week_start` is a function that maps a date string "YYYY-MM-DD" to its
    /// ISO week key "YYYY-Www".
    pub fn weekly_totals(
        &self,
        week_key: impl Fn(&str) -> String,
    ) -> std::collections::BTreeMap<String, TokenUsage> {
        let mut weeks: std::collections::BTreeMap<String, TokenUsage> = Default::default();
        for (date, usage) in &self.by_day {
            weeks.entry(week_key(date)).or_default().add(usage);
        }
        weeks
    }

    /// Derive monthly totals from `by_day` buckets (spec §10.2).
    pub fn monthly_totals(&self) -> std::collections::BTreeMap<String, TokenUsage> {
        let mut months: std::collections::BTreeMap<String, TokenUsage> = Default::default();
        for (date, usage) in &self.by_day {
            let month = &date[..7];
            months.entry(month.to_string()).or_default().add(usage);
        }
        months
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_usage_add() {
        let mut a = TokenUsage::new(100, 50);
        let b = TokenUsage::new(200, 30);
        a.add(&b);
        assert_eq!(a.input, 300);
        assert_eq!(a.output, 80);
        assert_eq!(a.total, 380);
    }

    #[test]
    fn context_ratio() {
        let usage = TokenUsage::new(15_400, 0);
        let ratio = usage.context_ratio(262_144);
        assert!((ratio - 0.0588).abs() < 0.001);
    }

    #[test]
    fn context_ratio_zero_window() {
        let usage = TokenUsage::new(100, 0);
        assert_eq!(usage.context_ratio(0), 0.0);
    }

    #[test]
    fn ledger_record_by_model() {
        let mut ledger = TokenLedger::default();
        ledger.record("openai/gpt-5.5", &TokenUsage::new(1000, 500));
        ledger.record("openai/gpt-5.5", &TokenUsage::new(2000, 100));
        ledger.record("cf/glm-4.7", &TokenUsage::new(50, 20));

        assert_eq!(ledger.total.input, 3050);
        assert_eq!(ledger.total.output, 620);
        assert_eq!(ledger.total.total, 3670);
        assert_eq!(ledger.by_model.len(), 2);
        let gpt = &ledger.by_model["openai/gpt-5.5"];
        assert_eq!(gpt.input, 3000);
        assert_eq!(gpt.output, 600);
    }

    #[test]
    fn monthly_totals_derived() {
        let mut ledger = DailyTokenLedger::default();
        ledger.record("2026-07-01", "openai/gpt-5.5", &TokenUsage::new(100, 10));
        ledger.record("2026-07-15", "openai/gpt-5.5", &TokenUsage::new(200, 20));
        ledger.record("2026-08-01", "openai/gpt-5.5", &TokenUsage::new(50, 5));

        let months = ledger.monthly_totals();
        assert_eq!(months.len(), 2);
        assert_eq!(months["2026-07"].input, 300);
        assert_eq!(months["2026-08"].input, 50);
    }
}
