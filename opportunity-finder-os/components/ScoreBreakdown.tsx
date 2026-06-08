import { getScoreBreakdown, type ScoreInput } from "@/lib/scoring";

export function ScoreBreakdown({ lead }: { lead: ScoreInput }) {
  const rows = getScoreBreakdown(lead);

  return (
    <div className="space-y-3">
      {rows.map((row) => (
        <div key={row.key}>
          <div className="mb-1 flex items-center justify-between text-sm">
            <span className="text-slate-300">{row.label}</span>
            <span className="font-medium text-white">
              {row.value}/5 · {row.points} pts
            </span>
          </div>
          <div className="h-2 rounded-full bg-slate-800">
            <div
              className="h-2 rounded-full bg-gradient-to-r from-teal-400 to-purple-400"
              style={{ width: `${(row.value / 5) * 100}%` }}
            />
          </div>
        </div>
      ))}
    </div>
  );
}
