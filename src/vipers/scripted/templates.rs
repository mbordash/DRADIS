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

//! The starters the Build a Viper page offers: a blank script and two small
//! worked examples. They are deliberately simple: they show the API (context
//! fields, parameters, state, the three things `exit` may return), not a
//! strategy anyone should run with money. The shipped Rust vipers cannot be
//! opened as scripts.

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct Template {
    pub id: &'static str,
    pub title: &'static str,
    pub summary: &'static str,
    pub manifest: serde_json::Value,
    pub source: &'static str,
}

pub const BLANK: &str = r#"// entry(ctx) is called every tick while the squadron has a market.
// Return () or #{ hold: "why" } to sit out, or an order:
//   #{ buy: "yes" | "no", price: ctx.yes.ask, shares: 5, take_profit: 0.60, reason: "..." }
fn entry(ctx) {
    #{ hold: "not written yet" }
}

// exit(ctx, pos) is called every tick for each position this viper holds.
// Return () to hold, #{ sell: ctx.yes.bid, reason: "..." } to sell as a taker,
// or #{ take_profit: 0.70 } to set or move the take-profit.
fn exit(ctx, pos) {
    ()
}
"#;

pub const MOMENTUM: &str = r#"// Plain momentum: buy the side the oracle is moving toward when the
// five-second move passes a threshold, with a take-profit and a stop.
fn entry(ctx) {
    if ctx.velocity == () { return #{ hold: "no price feed on this squadron" }; }
    if ctx.secs_to_close == () || ctx.secs_to_close < ctx.params.min_secs_to_close {
        return #{ hold: "too close to the close" };
    }
    // ctx.velocity is the oracle's move over five seconds in dollars; the
    // threshold is a fraction of the price, so it means the same at any level
    // (0.001 is $100 at $100,000), as in the built-in Momentum.
    let v = ctx.velocity;
    let thr = ctx.params.velocity * ctx.oracle;
    let side = if v > thr { "yes" } else if v < -thr { "no" } else { () };
    if side == () { return #{ hold: `5s move $${v} inside ±$${thr.round(2)}` }; }

    let book = if side == "yes" { ctx.yes } else { ctx.no };
    if book.ask < ctx.params.min_price || book.ask > ctx.params.max_price {
        return #{ hold: `ask ${book.ask} outside ${ctx.params.min_price}-${ctx.params.max_price}` };
    }
    // One entry per market: remember the last market entered.
    if this.entered == ctx.market_name { return #{ hold: "already entered this market" }; }
    this.entered = ctx.market_name;

    #{ buy: side, price: book.ask, shares: ctx.params.contracts,
       take_profit: min(book.ask + ctx.params.take_profit, 0.99),
       reason: `5s move $${v}` }
}

fn exit(ctx, pos) {
    let book = if pos.side == "yes" { ctx.yes } else { ctx.no };
    // The stop waits out the bounce that follows the spike it bought: a stop
    // armed from the first second sells that bounce, at a loss, and pays the
    // fee twice. It is a fraction of the entry, so it means the same on a 0.20
    // contract as on a 0.60 one.
    if pos.held_secs < ctx.params.min_hold_secs { return (); }
    let stop = pos.avg_entry * (1 - ctx.params.stop_loss_pct);
    if book.bid > 0 && book.bid <= stop {
        return #{ sell: book.bid, reason: `stop: bid ${book.bid} at or below ${stop.round(2)}, entry ${pos.avg_entry}, held ${pos.held_secs}s` };
    }
    ()
}
"#;

pub const LATE_FAVOURITE: &str = r#"// Late favourite: in the last minutes of a crypto market, when the oracle
// sits well clear of the strike, buy the side it favours and hold it to the
// settlement. A worked example of a fair value from norm_cdf.
//
// The venue settles on its own index, which can sit tens of dollars from the
// feed in ctx.oracle (Kalshi settles on CF Benchmarks). Near the strike that
// gap decides the trade, which is why min_fair keeps this to sides well clear
// of it.
fn entry(ctx) {
    if ctx.oracle == () || ctx.strike == () || ctx.secs_to_close == () {
        return #{ hold: "needs an oracle, a strike and a close time" };
    }
    let t = ctx.secs_to_close;
    if t > ctx.params.window_secs || t < 30 { return #{ hold: `outside the last ${ctx.params.window_secs}s` }; }

    // Distance to the strike in standard deviations over the time left. The
    // typical one-hour move is your parameter (a fraction of the price), scaled
    // by the square root of the time left. ctx.hist_vol is a 0-1 flatness
    // index, not a move, so it is not used here.
    let move_left = ctx.params.hourly_vol * ctx.oracle * (to_decimal(t) / 3600).sqrt();
    let z = (ctx.oracle - ctx.strike) / move_left;
    let p_yes = norm_cdf(z);
    let side = if p_yes > 0.5 { "yes" } else { "no" };
    let fair = if side == "yes" { p_yes } else { 1 - p_yes };
    // Big moves come several times as often as the normal curve says, so the
    // model may not claim more than fair_cap.
    if fair > ctx.params.fair_cap { fair = ctx.params.fair_cap; }
    if fair < ctx.params.min_fair { return #{ hold: `fair ${fair.round(3)} below ${ctx.params.min_fair} (z ${z.round(2)})` }; }

    let book = if side == "yes" { ctx.yes } else { ctx.no };
    if book.ask > ctx.params.max_price { return #{ hold: `ask ${book.ask} above ${ctx.params.max_price}` }; }
    let edge = fair - book.ask - taker_fee(book.ask);
    if edge < ctx.params.min_edge { return #{ hold: `edge ${edge.round(3)} below ${ctx.params.min_edge}` }; }

    #{ buy: side, price: book.ask, shares: ctx.params.contracts,
       reason: `fair ${fair.round(3)}, edge ${edge.round(3)}, oracle ${ctx.oracle} vs strike ${ctx.strike}, z ${z.round(2)}` }
}

// Held to the settlement: nothing to decide.
fn exit(ctx, pos) {
    ()
}
"#;

pub fn all() -> Vec<Template> {
    vec![
        Template {
            id: "blank",
            title: "Blank",
            summary: "Both functions, holding. Start here.",
            manifest: serde_json::json!({
                "name": "my_viper", "display_name": "My Viper", "description": "",
                "classes": ["crypto"], "venues": [], "max_exposure_usdc": 5, "params": {}
            }),
            source: BLANK,
        },
        Template {
            id: "momentum",
            title: "Plain Momentum",
            summary: "Buys the side the oracle is moving toward on a velocity threshold; take-profit, and a stop armed after a minimum hold.",
            manifest: serde_json::json!({
                "name": "my_momentum", "display_name": "My Momentum",
                "description": "Velocity threshold, taker in and out.",
                "classes": ["crypto"], "venues": [], "max_exposure_usdc": 5,
                "params": {
                    "velocity": { "type": "decimal", "default": 0.001, "min": 0.0001, "max": 0.05,
                                  "description": "Five-second oracle move that triggers an entry, as a fraction of the price (0.001 = $100 at $100,000)." },
                    "min_price": { "type": "decimal", "default": 0.20, "min": 0.01, "max": 0.99, "description": "Lowest ask to buy." },
                    "max_price": { "type": "decimal", "default": 0.70, "min": 0.01, "max": 0.99, "description": "Highest ask to buy." },
                    "take_profit": { "type": "decimal", "default": 0.10, "min": 0.01, "max": 0.5, "description": "Take-profit above the entry, in dollars." },
                    "stop_loss_pct": { "type": "decimal", "default": 0.35, "min": 0.05, "max": 0.9, "description": "Stop when the bid falls this fraction below the entry (0.35: a 0.40 entry stops at 0.26)." },
                    "min_hold_secs": { "type": "int", "default": 60, "min": 0, "max": 3600, "description": "The stop is not armed until the position has been held this long, so it does not sell the bounce after the spike it bought." },
                    "contracts": { "type": "int", "default": 5, "min": 1, "max": 100, "description": "Contracts per entry." },
                    "min_secs_to_close": { "type": "int", "default": 300, "min": 0, "max": 3600, "description": "No entry closer to the close than this." }
                }
            }),
            source: MOMENTUM,
        },
        Template {
            id: "late_favourite",
            title: "Late Favourite",
            summary: "Last-minutes fair value from norm_cdf; buys a clear favourite with an edge and holds to settlement.",
            manifest: serde_json::json!({
                "name": "my_late_favourite", "display_name": "My Late Favourite",
                "description": "Fair value from distance to strike; clear favourites only; hold to settlement.",
                "classes": ["crypto"], "venues": [], "max_exposure_usdc": 5,
                "params": {
                    "window_secs": { "type": "int", "default": 600, "min": 60, "max": 3600, "description": "Enter only in this many final seconds." },
                    "hourly_vol": { "type": "decimal", "default": 0.003, "min": 0.0005, "max": 0.05, "description": "Typical one-hour move of the underlying, as a fraction of the price (0.003 = 0.3%). It changes with the regime: BTC ran 0.13% to 0.41% an hour in November 2026. Too low makes every favourite look certain." },
                    "min_fair": { "type": "decimal", "default": 0.75, "min": 0.5, "max": 0.99, "description": "Only buy a side the model rates at least this likely; near the strike the feed-to-index gap decides the trade." },
                    "fair_cap": { "type": "decimal", "default": 0.95, "min": 0.6, "max": 0.999, "description": "Highest probability the model may claim. Big moves come several times as often as the normal curve says." },
                    "min_edge": { "type": "decimal", "default": 0.04, "min": 0, "max": 0.5, "description": "Fair minus ask minus fee required." },
                    "max_price": { "type": "decimal", "default": 0.90, "min": 0.01, "max": 0.99, "description": "Highest ask to buy." },
                    "contracts": { "type": "int", "default": 5, "min": 1, "max": 100, "description": "Contracts per entry." }
                }
            }),
            source: LATE_FAVOURITE,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vipers::scripted::manifest::Manifest;

    #[test]
    fn every_template_compiles_and_its_manifest_is_valid() {
        for t in all() {
            let m: Manifest = serde_json::from_value(t.manifest.clone()).unwrap_or_else(|e| panic!("{}: {e}", t.id));
            assert!(m.problems().is_empty(), "{}: {:?}", t.id, m.problems());
            crate::vipers::scripted::store::check_for_save(&m, t.source).unwrap_or_else(|e| panic!("{}: {e:?}", t.id));
        }
    }
}
