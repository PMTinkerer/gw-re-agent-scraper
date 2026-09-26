# Approved sparse-discovery release

Lucas answered "Go" to publishing the fix, one controlled all-town refresh,
application verification, and morning activation only after acceptance. Interpret
the bounded test extension as the same maximum previously approved: one run,
5,000 reserved units / at most 1,000 requests. Prior reservations remain intact;
the USD 5/month provider cap is unchanged. No email activation is authorized.

The existing whole-run ledger has 10,000 reserved units. Add a dated, single-use
approval with explicit daily/rolling ceilings of 15,000, applicable only while
that exact approval is being consumed in manual finalization mode. Do not change
normal constants, rewrite/refund history or automatically permit later tests.
Per-request accounting remains under its existing limits and balance checks.

1. Test the explicit ceiling exception before implementing it; validate dates,
   type/range, missing halves, mode, single use, and retained history.
2. Run offline regression/review, publish exact reviewed files to GitHub main,
   then verify the remote SHA and disabled daily gate.
3. Temporarily enable manual canary, dispatch exactly once, restore it off once
   the job starts. Preserve terminal diagnostics and all reservations on failure.
4. Only on a complete successful producer run: audit/backup the local sandbox,
   import through the pinned frozen-helper integration, verify new/inactive homes
   and unchanged projections/history/send state. Do not use the source DB directly.
5. Assess remaining morning-operation gates before activation. Never interpret
   an incomplete producer or merely green unit tests as operational acceptance.
