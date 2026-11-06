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

//! The sandbox a scripted viper runs in, and the translation of what a script
//! returns into something the adapter can check.
//!
//! One `Engine` for the process. The language has no file, network or clock
//! API in this build (`no_time`, `no_module`, `eval` disabled), and every call
//! is bounded in operations, call depth and the size of what it can build, so
//! a runaway script costs about half a millisecond and returns an error rather
//! than eating the patrol's per-strategy budget. Numbers with a decimal point
//! are `rust_decimal::Decimal` (`no_float`), the type the engine trades in.

use std::sync::OnceLock;

use rhai::packages::Package;
use rhai::{Dynamic, Engine, EvalAltResult, Map, Scope, AST};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;

/// Operations one call may spend. About half a millisecond on the prototype.
pub const MAX_OPERATIONS: u64 = 50_000;

/// What one script call asked for. Nothing here has been checked yet: the
/// adapter decides whether it becomes a signal.
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    /// Sit out. The text, when given, is the refusal reason on the viper card.
    Hold(Option<String>),
    /// Take the given side at `price` as a taker.
    Buy { side: Side, price: Decimal, shares: Decimal, take_profit: Option<Decimal>, reason: Option<String> },
    /// Sell the position at `price` as a taker.
    Sell { price: Decimal, reason: Option<String> },
    /// Rest (or move) the position's take-profit at `price`.
    TakeProfit { price: Decimal },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side { Yes, No }

impl Side {
    pub fn as_str(self) -> &'static str { match self { Side::Yes => "yes", Side::No => "no" } }
}

/// The shared engine.
pub fn engine() -> &'static Engine {
    static ENGINE: OnceLock<Engine> = OnceLock::new();
    ENGINE.get_or_init(build_engine)
}

fn build_engine() -> Engine {
    let mut e = Engine::new_raw();
    e.register_global_module(rhai::packages::StandardPackage::new().as_shared_module());
    e.set_max_operations(MAX_OPERATIONS);
    e.set_max_call_levels(16);
    e.set_max_expr_depths(64, 32);
    e.set_max_string_size(4_096);
    e.set_max_array_size(1_024);
    e.set_max_map_size(256);
    e.disable_symbol("eval");
    e.on_print(|_| {});
    e.on_debug(|_, _, _| {});
    register_helpers(&mut e);
    e
}

/// The documented helper functions. Each is part of the script API: adding
/// one is cheap, changing one breaks operators' scripts.
fn register_helpers(e: &mut Engine) {
    // Each helper takes either kind of number: Rhai does not widen an integer
    // to a decimal in a function call, so `clamp(x, 0, 1)` would otherwise be
    // "function not found", the first thing an operator would trip on.
    e.register_fn("taker_fee", |p: Dynamic| num(p).map(crate::venues::taker_fee_per_share));
    e.register_fn("round_tick", |p: Dynamic| num(p).map(crate::helpers::price::round_to_tick_size));
    e.register_fn("norm_cdf", |z: Dynamic| num(z).map(|z| {
        Decimal::from_f64(norm_cdf(z.to_f64().unwrap_or(0.0))).unwrap_or(Decimal::ZERO).round_dp(7)
    }));
    e.register_fn("clamp", |x: Dynamic, lo: Dynamic, hi: Dynamic| -> Result<Decimal, Box<EvalAltResult>> {
        Ok(num(x)?.max(num(lo)?).min(num(hi)?))
    });
    e.register_fn("min", |a: Dynamic, b: Dynamic| -> Result<Decimal, Box<EvalAltResult>> { Ok(num(a)?.min(num(b)?)) });
    e.register_fn("max", |a: Dynamic, b: Dynamic| -> Result<Decimal, Box<EvalAltResult>> { Ok(num(a)?.max(num(b)?)) });
    // `ctx.secs_to_close` and friends are integers; a script often wants them
    // in a Decimal formula.
    e.register_fn("to_decimal", |i: Dynamic| num(i));
}

/// A script number as a Decimal, whichever kind it is.
fn num(v: Dynamic) -> Result<Decimal, Box<EvalAltResult>> {
    if let Ok(d) = v.as_decimal() { return Ok(d); }
    if let Ok(i) = v.as_int() { return Ok(Decimal::from(i)); }
    Err(format!("expected a number, got {}", v.type_name()).into())
}

/// Abramowitz–Stegun 7.1.26 through erf; good to about 1e-7, plenty for a fee
/// comparison in cents.
pub fn norm_cdf(z: f64) -> f64 {
    let x = z / std::f64::consts::SQRT_2;
    let t = 1.0 / (1.0 + 0.327_591_1 * x.abs());
    let y = 1.0 - (((((1.061_405_429 * t - 1.453_152_027) * t) + 1.421_413_741) * t - 0.284_496_736) * t + 0.254_829_592) * t * (-x * x).exp();
    let erf = if x >= 0.0 { y } else { -y };
    0.5 * (1.0 + erf)
}

/// Compile a script and check that it defines what the adapter calls.
pub fn compile(source: &str) -> Result<AST, String> {
    let ast = engine().compile(source).map_err(|e| e.to_string())?;
    let has = |name: &str, arity: usize| ast.iter_functions().any(|f| f.name == name && f.params.len() == arity);
    let mut missing = Vec::new();
    if !has("entry", 1) { missing.push("fn entry(ctx)"); }
    if !has("exit", 2) { missing.push("fn exit(ctx, pos)"); }
    if !missing.is_empty() {
        return Err(format!("the script must define {}", missing.join(" and ")));
    }
    Ok(ast)
}

/// A short, stable fingerprint of a script's source, stamped on every paper row
/// and trade reason so an edit is visible in the record.
pub fn source_hash(source: &str) -> String {
    use sha2::{Digest, Sha256};
    let d = Sha256::digest(source.as_bytes());
    d.iter().take(4).map(|b| format!("{b:02x}")).collect()
}

/// Call `entry(ctx)` with `state` bound as `this`.
pub fn call_entry(ast: &AST, ctx: Map, state: &mut Dynamic) -> Result<Action, String> {
    let out = call(ast, "entry", (ctx,), state)?;
    parse_entry(out)
}

/// Call `exit(ctx, pos)` with `state` bound as `this`.
pub fn call_exit(ast: &AST, ctx: Map, pos: Map, state: &mut Dynamic) -> Result<Action, String> {
    let out = call(ast, "exit", (ctx, pos), state)?;
    parse_exit(out)
}

fn call(ast: &AST, f: &str, args: impl rhai::FuncArgs, state: &mut Dynamic) -> Result<Dynamic, String> {
    if !state.is_map() { *state = Map::new().into(); }
    let opts = rhai::CallFnOptions::new().bind_this_ptr(state).eval_ast(false);
    engine()
        .call_fn_with_options::<Dynamic>(opts, &mut Scope::new(), ast, f, args)
        .map_err(|e| format!("{f}(): {e}"))
}

fn decimal_of(m: &Map, key: &str) -> Result<Option<Decimal>, String> {
    match m.get(key) {
        None => Ok(None),
        Some(v) if v.is_unit() => Ok(None),
        Some(v) => {
            if let Ok(d) = v.as_decimal() { return Ok(Some(d)); }
            if let Ok(i) = v.as_int() { return Ok(Some(Decimal::from(i))); }
            Err(format!("'{key}' must be a number, got {}", v.type_name()))
        }
    }
}

fn reason_of(m: &Map) -> Option<String> {
    m.get("reason").filter(|v| !v.is_unit()).map(|v| v.to_string())
}

fn hold_of(m: &Map) -> Option<Action> {
    m.get("hold").map(|v| Action::Hold(if v.is_unit() { None } else { Some(v.to_string()) }))
}

fn as_map(out: Dynamic, f: &str) -> Result<Option<Map>, String> {
    if out.is_unit() { return Ok(None); }
    out.try_cast::<Map>()
        .map(Some)
        .ok_or_else(|| format!("{f}() must return () or a map like #{{ hold: \"...\" }}"))
}

pub fn parse_entry(out: Dynamic) -> Result<Action, String> {
    let Some(m) = as_map(out, "entry")? else { return Ok(Action::Hold(None)) };
    if let Some(h) = hold_of(&m) { return Ok(h); }
    let Some(side) = m.get("buy") else {
        return Err("entry() returned a map with neither 'hold' nor 'buy'".into());
    };
    let side = match side.to_string().to_ascii_lowercase().as_str() {
        "yes" => Side::Yes,
        "no" => Side::No,
        other => return Err(format!("'buy' must be \"yes\" or \"no\", got \"{other}\"")),
    };
    let price = decimal_of(&m, "price")?.ok_or("a buy needs 'price'")?;
    let shares = decimal_of(&m, "shares")?.ok_or("a buy needs 'shares'")?;
    Ok(Action::Buy { side, price, shares, take_profit: decimal_of(&m, "take_profit")?, reason: reason_of(&m) })
}

pub fn parse_exit(out: Dynamic) -> Result<Action, String> {
    let Some(m) = as_map(out, "exit")? else { return Ok(Action::Hold(None)) };
    if let Some(h) = hold_of(&m) { return Ok(h); }
    if let Some(price) = decimal_of(&m, "sell")? {
        return Ok(Action::Sell { price, reason: reason_of(&m) });
    }
    if let Some(price) = decimal_of(&m, "take_profit")? {
        return Ok(Action::TakeProfit { price });
    }
    Err("exit() returned a map with none of 'hold', 'sell' or 'take_profit'".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn ctx() -> Map {
        let mut m = Map::new();
        let mut yes = Map::new();
        yes.insert("ask".into(), Dynamic::from_decimal(dec!(0.42)));
        yes.insert("bid".into(), Dynamic::from_decimal(dec!(0.41)));
        m.insert("yes".into(), yes.into());
        m.insert("secs_to_close".into(), (1200 as rhai::INT).into());
        m.insert("now_secs".into(), (1000 as rhai::INT).into());
        m
    }

    const SCRIPT: &str = r#"
        fn entry(ctx) {
            if ctx.secs_to_close < 300 { return #{ hold: "too late" }; }
            let edge = 0.47 - ctx.yes.ask - taker_fee(ctx.yes.ask);
            if edge < 0 { return #{ hold: `edge ${edge}` }; }
            if this.last != () && ctx.now_secs - this.last < 300 { return #{ hold: "cooldown" }; }
            this.last = ctx.now_secs;
            #{ buy: "yes", price: ctx.yes.ask, shares: 5, take_profit: ctx.yes.ask + 0.10, reason: "edge" }
        }
        fn exit(ctx, pos) {
            if ctx.yes.bid < pos.avg_entry - 0.15 { return #{ sell: ctx.yes.bid, reason: "stop" }; }
            ()
        }
    "#;

    #[test]
    fn a_script_buys_exactly_and_keeps_its_state_between_calls() {
        let ast = compile(SCRIPT).unwrap();
        let mut state = Dynamic::UNIT;
        let a = call_entry(&ast, ctx(), &mut state).unwrap();
        assert_eq!(a, Action::Buy {
            side: Side::Yes, price: dec!(0.42), shares: dec!(5),
            take_profit: Some(dec!(0.52)), reason: Some("edge".into()),
        }, "decimal arithmetic is exact and an integer share count is accepted");
        assert_eq!(call_entry(&ast, ctx(), &mut state).unwrap(), Action::Hold(Some("cooldown".into())),
            "`this` persists between calls");
    }

    #[test]
    fn exit_maps_sell_and_hold() {
        let ast = compile(SCRIPT).unwrap();
        let mut pos = Map::new();
        pos.insert("avg_entry".into(), Dynamic::from_decimal(dec!(0.60)));
        let a = call_exit(&ast, ctx(), pos.clone(), &mut Dynamic::UNIT).unwrap();
        assert_eq!(a, Action::Sell { price: dec!(0.41), reason: Some("stop".into()) });
        pos.insert("avg_entry".into(), Dynamic::from_decimal(dec!(0.45)));
        assert_eq!(call_exit(&ast, ctx(), pos, &mut Dynamic::UNIT).unwrap(), Action::Hold(None));
    }

    #[test]
    fn a_runaway_script_is_stopped_and_reported() {
        let ast = compile("fn entry(ctx) { loop {} } fn exit(ctx, pos) { () }").unwrap();
        let t = std::time::Instant::now();
        let err = call_entry(&ast, ctx(), &mut Dynamic::UNIT).unwrap_err();
        assert!(err.contains("Too many operations"), "{err}");
        assert!(t.elapsed().as_millis() < 100, "stopped quickly, took {:?}", t.elapsed());
    }

    #[test]
    fn the_sandbox_has_no_eval_import_or_clock_and_requires_both_functions() {
        assert!(compile("fn entry(ctx) { eval(\"1\") } fn exit(ctx, pos) { () }").is_err());
        assert!(compile("import \"x\" as y; fn entry(ctx) { () } fn exit(ctx, pos) { () }").is_err());
        let ast = compile("fn entry(ctx) { timestamp() } fn exit(ctx, pos) { () }").unwrap();
        assert!(call_entry(&ast, ctx(), &mut Dynamic::UNIT).is_err(), "no clock but ctx.now_secs");
        let e = compile("fn entry(ctx) { () }").unwrap_err();
        assert!(e.contains("fn exit(ctx, pos)"), "{e}");
        let e = compile("fn entry(ctx) { let x = ; }").unwrap_err();
        assert!(e.contains("line 1"), "a syntax error names its line: {e}");
    }

    #[test]
    fn malformed_returns_are_errors_not_orders() {
        for (src, want) in [
            ("#{ buy: \"maybe\", price: 0.4, shares: 5 }", "\"yes\" or \"no\""),
            ("#{ buy: \"yes\", shares: 5 }", "needs 'price'"),
            ("#{ buy: \"yes\", price: \"cheap\", shares: 5 }", "must be a number"),
            ("#{ foo: 1 }", "neither 'hold' nor 'buy'"),
            ("42", "must return ()"),
        ] {
            let ast = compile(&format!("fn entry(ctx) {{ {src} }} fn exit(ctx, pos) {{ () }}")).unwrap();
            let e = call_entry(&ast, ctx(), &mut Dynamic::UNIT).unwrap_err();
            assert!(e.contains(want), "{src}: {e}");
        }
    }

    /// The decimal methods the Control Tower's Reference tab documents.
    #[test]
    fn the_documented_decimal_methods_exist() {
        let ast = compile("fn entry(ctx) { let x = 2.25; #{ hold: `${x.sqrt().round(2)} ${(-x).abs()} ${x.round(1)} ${x.ln().round(3)} ${0.5.exp().round(3)}` } } fn exit(ctx, pos) { () }").unwrap();
        let Action::Hold(Some(s)) = call_entry(&ast, ctx(), &mut Dynamic::UNIT).unwrap() else { panic!() };
        assert_eq!(s, "1.50 2.25 2.2 0.811 1.649", "round() takes halves to the even digit: 2.25 → 2.2");
    }

    #[test]
    fn helpers_are_callable() {
        let ast = compile("fn entry(ctx) { #{ hold: `${round_tick(0.4249)} ${clamp(1.5, 0, 1)} ${norm_cdf(0)} ${min(2, 0.5)} ${taker_fee(0.5) * 100}` } } fn exit(ctx, pos) { () }").unwrap();
        let Action::Hold(Some(s)) = call_entry(&ast, ctx(), &mut Dynamic::UNIT).unwrap() else { panic!() };
        assert_eq!(s, format!("0.42 1 0.5000000 0.5 {}", crate::venues::taker_fee_per_share(rust_decimal_macros::dec!(0.5)) * rust_decimal_macros::dec!(100)));
        assert!((norm_cdf(1.96) - 0.975).abs() < 1e-4);
    }
}
