# Stock decision explanation accuracy

This is a separate agent explanation defect, not an IBKR data repair.
The real-model gate run 38015216000 on 494e7f2ca5f82f1d6ed5cceb7455a082f773367e
completed 51 repetitions: 50 passed, one failed, none missing or errored.
Estimated model/judge cost was $0.248636, with fixture-only external writes.

The unchanged judge rejected repetition 3 of stock_order_decision_journal.
The order size, single submission and pending-status report were correct.
The order explanation claimed that every one of the closes 225, 226, 227,
228 and 229 exceeded their 227 average. That claim was false. The strategy
required the current price to exceed the average; the extra claim was not
needed to justify the order. The failure is retained as evidence rather than
classified as provider noise or erased by an unchanged retry.

The general stock skill now distinguishes the actual evaluated comparison
from additional claims about all members of a series. Such additional claims
require the corresponding computed predicate. No strategy rule, model, order
permission, sizing constraint, judge rubric or reason-length contract changes.
The standalone gate supports five targeted repetitions for this failed case;
all five must pass before a final publication gate can qualify the candidate.

Qualification status: pending targeted five-repeat real-model proof.
