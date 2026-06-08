import { LeadForm } from "@/components/LeadForm";
import { createLead } from "@/app/leads/actions";

export default function NewLeadPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold tracking-tight text-white">Add a lead</h1>
        <p className="mt-2 max-w-3xl text-slate-400">
          Add a consulting, fractional, startup, family office, or institutional opportunity. Scoring and offer
          recommendations are generated locally when you save.
        </p>
      </div>
      <LeadForm action={createLead} submitLabel="Create and score lead" />
    </div>
  );
}
