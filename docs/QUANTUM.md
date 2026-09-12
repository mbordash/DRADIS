# DRADIS Quantum Research Track: Combinatorial Prediction Markets

**Status: exploratory research proposal.** Nothing described here is implemented, and nothing here sits in DRADIS's live trading path. This document defines a question, the classical baselines that have to be beaten, and the conditions under which the quantum work stops. Results will be published in this repository whether they are positive or negative.

---

## The question

Prediction markets list contracts that each settle at $0 or $1. It is tempting to conclude that this makes them a natural fit for qubits. On its own it doesn't, and the reason is worth stating up front because it points to where a real connection might be.

The obvious mapping puts binary variables on *trading decisions*: hold this contract or not. But any yes/no decision problem can be written as a Quadratic Unconstrained Binary Optimization (QUBO), so this says nothing special about prediction markets. In practice the decision problems DRADIS faces are also small (tens to a few hundred candidate positions), real position sizes are not binary, and classical solvers handle problems of that size in milliseconds. A quantum solver has nothing to win there.

The binary structure matters somewhere else: in the space of **outcomes**. When many contracts are logically linked, the set of ways the world can resolve grows exponentially, and finding a mispricing means searching that space. Each possible world is a bit string of event outcomes, and the logical links between events are constraints on that bit string. That search is where this track looks for a quantum role.

**Research question:** For sets of logically linked prediction market contracts, at what scale does searching the outcome space for arbitrage become too slow to do classically within the lifetime of the opportunity, and can quantum optimization close that gap?

---

## Where the hardness lives

Linked contracts are common on real venues:

* **Mutually exclusive outcomes.** An election or tournament market lists each candidate or team as a separate YES/NO contract, and exactly one resolves YES. The YES prices should sum to about $1.
* **Implications.** "Wins the division" implies "makes the playoffs." A state-level result constrains the national result.
* **The same event on two venues.** Polymarket and Kalshi may both list an event, with prices that should agree. (Their resolution rules can differ, which is a real risk no formulation captures.)

When the linked set is small, checking for arbitrage is easy: enumerate every consistent outcome and solve a linear program. The difficulty is combinatorial, and it is well documented on the market maker's side. Chen, Fortnow, Lambert, Pennock and Wortman showed that pricing combinatorial prediction markets under Hanson's logarithmic market scoring rule is #P-hard even for restricted betting languages [1]. Kroer, Dudík, Lahaie and Balakrishnan then built a practical arbitrage-free combinatorial market maker, demonstrated on NCAA tournament markets with 2^63 possible outcomes, whose key step calls an **integer programming oracle** that optimizes over the consistent outcomes [2].

This proposal works on the other side of the market. A trader facing independent per-contract order books is not running a market maker and has no cost function to project onto, so neither result transfers directly. What carries over is the primitive: a search over consistent outcome bit strings, which [2] shows can be solved well classically at large scale. That sets the bar any quantum claim has to meet.

---

## Formulation

Let contracts $i = 1, \dots, n$ each pay \$1 if event $E_i$ occurs, and let $\omega \in \{0,1\}^m$ be the outcomes of the $m$ underlying events. Let $\Omega$ be the set of outcome strings consistent with the logical constraints.

Buying $q_i \ge 0$ shares at ask $a_i$ with per-share fee $f_i$ locks in arbitrage if the portfolio profits in every consistent world:

$$\max_{q} \; \min_{\omega \in \Omega} \; \sum_{i} q_i \left( \mathbb{1}[\omega \in E_i] - a_i - f_i \right)$$

subject to depth limits $q_i \le d_i$ and a capital budget $\sum_i q_i (a_i + f_i) \le B$. (A flat per-share fee and a single depth cap are simplifications; walking several price levels makes the cost piecewise linear, which fits the same framework.)

Written over an explicit $\Omega$, this is a linear program. It becomes hard because $\Omega$ is exponentially large, and the standard remedy is constraint generation: solve over a small set of outcomes, ask an oracle for the consistent outcome in which the current portfolio does worst, add it, and repeat. This is the same kind of oracle [2] uses inside its market maker's pricing loop, applied here to a trader's portfolio.

**The oracle is the quantum candidate.** It is a search over binary outcome strings with an objective that is linear when each contract references one event and quadratic when contracts are conjunctions of two events, subject to logical constraints that map onto QUBO penalty terms in the standard way [3]:

* Exactly one of a mutually exclusive set: $\lambda \left( \sum_{j \in S} \omega_j - 1 \right)^2$
* Implication $A \Rightarrow B$: $\lambda \, \omega_A (1 - \omega_B)$

The oracle problem is then

$$\min_{\omega \in \{0,1\}^m} \; \omega^T Q \, \omega + c^T \omega$$

where $c$ and $Q$ carry the current portfolio's exposure to each outcome plus the constraint penalties. This is a 0/1 integer program, NP-hard in general, which is the standard target class for the Quantum Approximate Optimization Algorithm (QAOA) [4] on gate-based hardware and for quantum annealing. The #P-hardness in [1] concerns market maker pricing and is not claimed for this oracle.

**Qubit counts.** One qubit per underlying event is a floor, not an estimate:

* Contracts that combine three or more events create higher-order terms, which need auxiliary qubits to reduce to quadratic form, and richer logical constraints can need more.
* An exactly-one penalty couples every pair of events in its set. On gate-based hardware with sparse connectivity, such as IBM's heavy-hex lattice, that means SWAP networks and deeper circuits. On annealers it means minor embedding, with chains of physical qubits standing in for each logical one.

---

## Plan and stopping rules

**Expected outcome.** The honest expectation is that this track stops at Phase 0 or Phase 1. Linked sets listed today range from a handful of contracts to perhaps a few hundred, far from the 2^63-outcome research setting in [2], and classical solvers handle problems of that size quickly. A clean negative result, published with its data and instances, is still useful: it shows anyone making quantum claims about prediction markets where the bar actually is. Collaborators should not expect Phases 2 and 3 to run.

### Phase 0: Measure the opportunity

Record linked contract sets from the venues' streaming order book feeds rather than periodic snapshots, so that opportunity lifetimes are measured at the resolution of the feed itself. Publish that time resolution with the results. Record depth at every price level, so a later replay can see what was actually available at any moment. From the recording, report:

* The number of linked contracts and underlying events per set.
* How often prices violate logical consistency by more than fees and available depth.
* How long each violation persists.

If violations net of fees are rare or vanish faster than any order could reach the venue, the track stops here and the measurement is published.

### Phase 1: Classical baselines

Implement the constraint generation approach above with several classical oracles: an exact integer programming solver, tuned metaheuristics (simulated annealing and tabu search), and quantum-inspired classical Ising solvers where available. Record detection time, from the book update to the identified trade, against instance size. The fastest baseline sets the classical detection time.

**Proposed stopping rule:** replay every recorded opportunity twice, once with the classical detection time and once with zero detection time. Both replays add the same measured execution latency, fill orders only against the depth the recording shows at the moment they would arrive, and count a trade spanning two venues only if every leg fills, since there is no atomic settlement across venues. If the classical replay captures at least 99% of the profit, net of fees, that the zero-detection-time replay captures, then even an instant solver could add at most 1% to what is capturable. The quantum phases stop, and the result is published along with the instance set.

The same replay reports the instance size at which classical detection would start losing more than 1% of that profit, so the result also says how far today's linked markets are from the point where quantum speed would matter. Opportunities shorter than the recording resolution are invisible to both replays; these are precisely the ones where speed matters most, so the published result states the resolution as a limit on its conclusion. The threshold is fixed here, before any data is collected, so the data cannot move it.

### Phase 2: Pre-registered quantum benchmark

Only if Phase 1 leaves a gap. The instance set, metrics and comparison methods are published before any quantum run.

* **Instances:** oracle problems from real recordings with 10 to about 30 binary variables. At this size every instance can be solved exactly by brute force, so Phase 2 measures correctness and cost. It cannot demonstrate an advantage at scale.
* **Methods:** QAOA on noiseless simulation, then on IBM Quantum hardware, against every Phase 1 baseline.
* **Metrics:** approximation ratio, probability of sampling the optimum, and total wall-clock time, including queueing and the classical optimization of QAOA's circuit parameters, which is often the dominant cost.

### Phase 3: Hybrid decomposition

Only if Phase 2 shows scaling that plausibly overtakes the classical baselines. Constraint generation stays classical, and quantum hardware answers oracle subproblems on the hardest linked sets.

---

## Tracks considered and set aside

* **Quantum Amplitude Estimation for joint event probabilities.** The full quadratic speedup over Monte Carlo needs error rates well below current hardware. Near-term variants avoid full phase estimation but have not shown a practical advantage, loading the distribution into the circuit consumes much of the gain, and the joint distributions DRADIS works with are small enough to compute cheaply today. Worth revisiting as hardware matures.
* **Quantum machine learning on market features.** There is no demonstrated advantage for quantum classifiers on classical data. DRADIS's own classical modeling work has been limited by data quantity, leakage and fees rather than model capacity, and a higher-dimensional feature map makes overfitting on small, noisy datasets worse.

---

## What this is not

DRADIS makes trading decisions on a sub-second loop, and cloud quantum jobs queue for minutes or longer. None of this work would ever sit in the execution path. Any useful output would be offline: a characterization of where combinatorial arbitrage exists, and whether its search problem is hard enough at real scale to benefit from quantum hardware.

---

## Tooling

* **Instances as plain data.** Each oracle problem is exported as its $Q$ and $c$ matrices with metadata, so any framework or hardware provider can run exactly the same instances.
* **Research harness in Python,** separate from the Rust trading engine.
* **Qiskit** for IBM Quantum hardware, with the same instances runnable through PennyLane, Amazon Braket or annealing services.

---

## Collaborators

This track needs people who will hold it to a high standard:

* **Combinatorial market and mechanism design researchers** to challenge the formulation and the Phase 0 measurements.
* **Quantum optimization researchers** to review the QUBO mappings and co-design the Phase 2 benchmark before it runs.
* **Hardware access** for Phase 2, if and only if Phase 1 justifies it.

To get involved, open an issue tagged `[Quantum Proposal]` or start a thread in GitHub Discussions.

---

## References

1. Y. Chen, L. Fortnow, N. Lambert, D. M. Pennock, J. Wortman. *Complexity of Combinatorial Market Makers.* ACM Conference on Electronic Commerce (EC), 2008. [arXiv:0802.1362](https://arxiv.org/abs/0802.1362)
2. C. Kroer, M. Dudík, S. Lahaie, S. Balakrishnan. *Arbitrage-Free Combinatorial Market Making via Integer Programming.* 2016. [arXiv:1606.02825](https://arxiv.org/abs/1606.02825)
3. A. Lucas. *Ising formulations of many NP problems.* Frontiers in Physics 2, 5 (2014). [arXiv:1302.5843](https://arxiv.org/abs/1302.5843)
4. E. Farhi, J. Goldstone, S. Gutmann. *A Quantum Approximate Optimization Algorithm.* 2014. [arXiv:1411.4028](https://arxiv.org/abs/1411.4028)
