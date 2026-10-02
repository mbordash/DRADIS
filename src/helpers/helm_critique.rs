// SPDX-License-Identifier: AGPL-3.0-only
//
// DRADIS — autonomous trading engine for crypto prediction markets.
// Copyright (C) 2026 Michael Bordash
//
// This file is part of DRADIS. DRADIS is free software: you can redistribute it
// and/or modify it under the terms of the GNU Affero General Public License,
// version 3, as published by the Free Software Foundation.
//
// DRADIS is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
// A PARTICULAR PURPOSE. See the GNU Affero General Public License for details.
//
// You should have received a copy of the GNU Affero General Public License along
// with this program. If not, see <https://www.gnu.org/licenses/>.

//! The Helm critique: one bounded, advisory reading of the operator's thesis.
//!
//! Three properties, each enforced here rather than by convention:
//!
//! 1. **It cannot block.** The outcome is recorded on the intent — a critique
//!    or `unavailable: <why>` — and the acknowledge route proceeds either way
//!    once the timeout has passed. A gate the operator must satisfy would teach
//!    them to write what passes, and the stored thesis would stop being what
//!    they believed.
//! 2. **It cannot delay past its timeout.** `run_with` wraps the call in
//!    `tokio::time::timeout` with the operator's `helm_critique_timeout_secs`;
//!    an overrun is an `Unavailable`, not a wait.
//! 3. **It sees no Raptor state.** The prompt is built from the intent's own
//!    fields, the market's name and the current price of the side — nothing
//!    from any signal source. Given the book and the signals it would become a
//!    second model with an opinion on the market; the role is a reader of the
//!    operator's reasoning.

use rust_decimal::Decimal;
use std::future::Future;
use std::time::Duration;
use tracing::{info, warn};

/// What the critique is given. Deliberately a closed struct: adding a field
/// here is the only way to show the model more, and the test below pins the
/// prompt to these fields.
#[derive(Clone, Debug)]
pub struct CritiqueInput {
    pub market_name: String,
    /// `YES` or `NO`.
    pub side: String,
    /// Current price of the side being bought (its ask), when the book had one.
    pub current_price: Option<Decimal>,
    pub thesis: String,
    pub confidence: f64,
    /// `expiry` or `sooner`.
    pub horizon: String,
    pub falsification: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CritiqueOutcome {
    Critique { text: String, model: String },
    Unavailable(String),
}

impl CritiqueOutcome {
    /// The text stored in `helm_intents.critique` and `critique_model`.
    pub fn stored(&self) -> (String, String) {
        match self {
            Self::Critique { text, model } => (text.clone(), model.clone()),
            Self::Unavailable(why) => (format!("unavailable: {why}"), "unavailable".to_string()),
        }
    }
}

/// The role, in full. Reads reasoning; does not read markets; does not decide.
pub fn system_prompt() -> &'static str {
    "You are a skeptical reviewer of a trader's written reasoning for one position in a binary prediction market. \
     You are given only what the trader wrote: their thesis, their stated probability, their horizon, the condition \
     they say would prove them wrong, the market's name, and the current price of the side they intend to buy. \
     You have no market data, no signals and no model of your own, and you must not pretend to. \
     Your job is to test the reasoning, not the market: name the strongest reason the thesis could be wrong that the \
     trader did not mention; say whether the stated probability is consistent with the price they are paying and with \
     the confidence the thesis itself supports; say whether the falsification condition is specific enough to be \
     observed and whether it is the condition that would actually show the thesis wrong. \
     Be concrete and brief: at most 150 words, plain prose, no headings, no bullet points, no recommendation to \
     trade or not to trade, no numbers you were not given."
}

/// The prompt, from the input and nothing else.
pub fn user_prompt(i: &CritiqueInput) -> String {
    let price = i.current_price.map_or("not available".to_string(), |p| format!("${p:.4}"));
    format!(
        "Market: {}\nSide the trader intends to buy: {}\nCurrent price of that side: {}\n\
         Stated probability the thesis resolves in the trader's favor: {:.0}%\nHorizon: {}\n\n\
         Thesis, in the trader's words:\n{}\n\nWhat the trader says would prove them wrong:\n{}\n",
        i.market_name, i.side.to_ascii_uppercase(), price, i.confidence * 100.0, i.horizon,
        i.thesis.trim(), i.falsification.trim(),
    )
}

/// Run any critique future under the timeout. Separated from `run` so the
/// timeout and error paths are testable without a provider.
pub async fn run_with<F>(call: F, timeout: Duration) -> CritiqueOutcome
where
    F: Future<Output = anyhow::Result<(String, String)>>,
{
    match tokio::time::timeout(timeout, call).await {
        Ok(Ok((text, model))) => {
            let text = text.trim().to_string();
            if text.is_empty() {
                CritiqueOutcome::Unavailable("the model returned an empty reply".into())
            } else {
                CritiqueOutcome::Critique { text, model }
            }
        }
        Ok(Err(e)) => CritiqueOutcome::Unavailable(e.to_string()),
        Err(_) => CritiqueOutcome::Unavailable(format!("timed out after {}s", timeout.as_secs())),
    }
}

/// The critique on the configured provider, bounded by `timeout`.
pub async fn run(input: &CritiqueInput, timeout: Duration) -> CritiqueOutcome {
    let user = user_prompt(input);
    run_with(crate::helpers::llm_advisor::one_shot(system_prompt(), &user), timeout).await
}

/// Request the critique for an intent and record the outcome on its row.
/// Spawned by the API so the request returns at once; the row says
/// `critique_requested_at` meanwhile, and the acknowledge route treats a row
/// still unanswered past the timeout as unavailable.
pub fn request(pool: sqlx::SqlitePool, intent_id: i64, input: CritiqueInput, timeout: Duration) {
    tokio::spawn(async move {
        if let Err(e) = crate::helpers::helm::set_critique_requested(&pool, intent_id).await {
            warn!("Helm critique #{intent_id}: could not mark requested: {e}");
        }
        let outcome = run(&input, timeout).await;
        let (text, model) = outcome.stored();
        match &outcome {
            CritiqueOutcome::Critique { model, .. } => info!("🧭 Helm critique #{intent_id} recorded ({model})"),
            CritiqueOutcome::Unavailable(why) => info!("🧭 Helm critique #{intent_id} unavailable: {why}"),
        }
        if let Err(e) = crate::helpers::helm::set_critique(&pool, intent_id, &text, &model).await {
            warn!("Helm critique #{intent_id}: could not record: {e}");
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn input() -> CritiqueInput {
        CritiqueInput {
            market_name: "Bitcoin Up or Down - October 2, 3PM ET".into(),
            side: "no".into(),
            current_price: Some(dec!(0.42)),
            thesis: "pinned under the strike into the close".into(),
            confidence: 0.65,
            horizon: "expiry".into(),
            falsification: "a NO print below 0.30".into(),
        }
    }

    /// The prompt is the input and nothing else: every field appears, and
    /// nothing that is not a field does. Adding Raptor state would have to
    /// change this struct and this test.
    #[test]
    fn the_prompt_is_built_from_the_input_fields_only() {
        let p = user_prompt(&input());
        let expected = "Market: Bitcoin Up or Down - October 2, 3PM ET\nSide the trader intends to buy: NO\n\
            Current price of that side: $0.4200\nStated probability the thesis resolves in the trader's favor: 65%\n\
            Horizon: expiry\n\nThesis, in the trader's words:\npinned under the strike into the close\n\n\
            What the trader says would prove them wrong:\na NO print below 0.30\n";
        assert_eq!(p, expected);
        let mut i = input();
        i.current_price = None;
        assert!(user_prompt(&i).contains("Current price of that side: not available"));
    }

    #[test]
    fn the_system_prompt_forbids_a_trade_recommendation_and_outside_data() {
        let s = system_prompt();
        assert!(s.contains("no recommendation to trade"));
        assert!(s.contains("no market data, no signals"));
        assert!(s.contains("at most 150 words"));
    }

    /// A call that outruns the timeout is unavailable, not awaited.
    #[tokio::test]
    async fn an_overrun_is_unavailable_at_the_timeout() {
        let slow = async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            Ok(("late".to_string(), "m".to_string()))
        };
        let started = std::time::Instant::now();
        let out = run_with(slow, Duration::from_millis(50)).await;
        assert_eq!(out, CritiqueOutcome::Unavailable("timed out after 0s".into()));
        assert!(started.elapsed() < Duration::from_secs(2), "did not wait for the slow call");
    }

    #[tokio::test]
    async fn an_error_and_an_empty_reply_are_unavailable_and_a_reply_is_a_critique() {
        let err = run_with(async { Err(anyhow::anyhow!("no key")) }, Duration::from_secs(1)).await;
        assert_eq!(err, CritiqueOutcome::Unavailable("no key".into()));
        let empty = run_with(async { Ok(("   ".to_string(), "m".to_string())) }, Duration::from_secs(1)).await;
        assert!(matches!(empty, CritiqueOutcome::Unavailable(_)));
        let ok = run_with(async { Ok((" fine ".to_string(), "anthropic/x".to_string())) }, Duration::from_secs(1)).await;
        assert_eq!(ok, CritiqueOutcome::Critique { text: "fine".into(), model: "anthropic/x".into() });
        assert_eq!(ok.stored(), ("fine".to_string(), "anthropic/x".to_string()));
        assert_eq!(err.stored(), ("unavailable: no key".to_string(), "unavailable".to_string()));
    }
}
