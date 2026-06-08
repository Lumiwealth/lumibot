import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { POSITIONING, REVENUE_ASSUMPTIONS, SCORE_LABELS, SCORE_WEIGHTS } from "@/lib/constants";
import { formatCurrency } from "@/lib/utils";

export default function SettingsPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold tracking-tight text-white">Settings</h1>
        <p className="mt-2 max-w-3xl text-slate-400">
          MVP assumptions are static constants for now. They are centralized in <code>lib/constants.ts</code> for later
          editing in the UI.
        </p>
      </div>

      <div className="grid gap-6 xl:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Scoring weights</CardTitle>
            <CardDescription>Each factor is entered on a 1-5 scale and converted to 0-100.</CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            {Object.entries(SCORE_WEIGHTS).map(([key, weight]) => (
              <div key={key} className="flex items-center justify-between rounded-xl border border-slate-800 bg-slate-950/50 p-3">
                <span className="text-sm text-slate-300">{SCORE_LABELS[key as keyof typeof SCORE_LABELS]}</span>
                <span className="font-semibold text-white">{Math.round(weight * 100)}%</span>
              </div>
            ))}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Revenue assumptions</CardTitle>
            <CardDescription>Used for forecast estimates only.</CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            <Setting label="Cold leads become calls" value={`${REVENUE_ASSUMPTIONS.coldLeadToCallRate * 100}%`} />
            <Setting label="Warm leads become calls" value={`${REVENUE_ASSUMPTIONS.warmLeadToCallRate * 100}%`} />
            <Setting label="Calls become proposals" value={`${REVENUE_ASSUMPTIONS.callToProposalRate * 100}%`} />
            <Setting label="Proposals close" value={`${REVENUE_ASSUMPTIONS.proposalCloseRate * 100}%`} />
            <Setting label="Average starter project" value={formatCurrency(REVENUE_ASSUMPTIONS.averageStarterProject)} />
            <Setting label="Average retainer" value={formatCurrency(REVENUE_ASSUMPTIONS.averageRetainer)} />
            <Setting label="Premium retainer" value={formatCurrency(REVENUE_ASSUMPTIONS.premiumRetainer)} />
            <Setting label="Institutional pilot/license" value={formatCurrency(REVENUE_ASSUMPTIONS.institutionalPilot)} />
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Positioning language</CardTitle>
          <CardDescription>Used by deterministic outreach templates.</CardDescription>
        </CardHeader>
        <CardContent className="grid gap-3 md:grid-cols-2">
          {Object.entries(POSITIONING).map(([key, value]) => (
            <div key={key} className="rounded-xl border border-slate-800 bg-slate-950/50 p-4">
              <div className="mb-2 text-xs font-semibold uppercase tracking-wide text-teal-200">{key}</div>
              <p className="text-sm leading-6 text-slate-300">{value}</p>
            </div>
          ))}
        </CardContent>
      </Card>
    </div>
  );
}

function Setting({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between rounded-xl border border-slate-800 bg-slate-950/50 p-3">
      <span className="text-sm text-slate-300">{label}</span>
      <span className="font-semibold text-white">{value}</span>
    </div>
  );
}
