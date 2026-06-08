import Link from "next/link";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { CONFIDENCE_LEVELS, LEAD_CATEGORIES, PIPELINE_STAGES, REVENUE_POTENTIALS } from "@/lib/constants";
import type { LeadRecord } from "@/lib/types";
import { formatDateInput } from "@/lib/utils";

export function LeadForm({
  action,
  defaultValues,
  submitLabel = "Save lead",
}: {
  action: (formData: FormData) => Promise<void>;
  defaultValues?: Partial<LeadRecord>;
  submitLabel?: string;
}) {
  const value = (key: keyof LeadRecord, fallback = "") => String(defaultValues?.[key] ?? fallback);
  const scale = (key: keyof LeadRecord, fallback = 3) => Number(defaultValues?.[key] ?? fallback);

  return (
    <form action={action} className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Lead profile</CardTitle>
          <CardDescription>Capture the context needed to decide if Rico should contact them today.</CardDescription>
        </CardHeader>
        <CardContent className="grid gap-4 md:grid-cols-2">
          <Field label="Company name" name="company_name" defaultValue={value("company_name")} required />
          <SelectField label="Category" name="category" defaultValue={value("category", "Fintech Startup")} options={LEAD_CATEGORIES} />
          <Field label="Website" name="website" defaultValue={value("website")} placeholder="https://example.com" />
          <Field label="Location" name="location" defaultValue={value("location")} placeholder="Remote, NYC, Atlanta..." />
          <Field label="Contact name" name="contact_name" defaultValue={value("contact_name")} />
          <Field label="Contact title" name="contact_title" defaultValue={value("contact_title")} />
          <Field label="Contact email" name="contact_email" type="email" defaultValue={value("contact_email")} placeholder="contact@example.com" />
          <Field label="LinkedIn URL" name="linkedin_url" defaultValue={value("linkedin_url")} />
          <Field label="Warm intro source" name="warm_intro_source" defaultValue={value("warm_intro_source")} />
          <Field label="Estimated budget" name="estimated_budget" defaultValue={value("estimated_budget")} placeholder="$5K/month, pilot budget..." />
          <div className="md:col-span-2">
            <Label htmlFor="personalized_angle">Personalized angle</Label>
            <Textarea
              id="personalized_angle"
              name="personalized_angle"
              defaultValue={value("personalized_angle")}
              placeholder="Why this company is a fit for LCS, decision intelligence, or AI product help."
            />
          </div>
          <div className="md:col-span-2">
            <Label htmlFor="notes">Notes</Label>
            <Textarea id="notes" name="notes" defaultValue={value("notes")} placeholder="Research notes, pain points, relationship context..." />
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Scoring factors</CardTitle>
          <CardDescription>Use a simple 1-5 scale. The app converts these to a 0-100 fit score.</CardDescription>
        </CardHeader>
        <CardContent className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          <ScaleField label="Ability to pay" name="ability_to_pay" defaultValue={scale("ability_to_pay")} />
          <ScaleField label="Fit with Rico's background" name="fit_with_my_background" defaultValue={scale("fit_with_my_background")} />
          <ScaleField label="Need for AI product help" name="need_for_ai_product_help" defaultValue={scale("need_for_ai_product_help")} />
          <ScaleField label="Relevance to LCS Engine" name="relevance_to_lcs" defaultValue={scale("relevance_to_lcs")} />
          <ScaleField label="Decision intelligence relevance" name="relevance_to_decision_intelligence" defaultValue={scale("relevance_to_decision_intelligence")} />
          <ScaleField label="Family office / wealth fit" name="family_office_or_wealth_fit" defaultValue={scale("family_office_or_wealth_fit")} />
          <ScaleField label="Institutional education fit" name="institutional_education_fit" defaultValue={scale("institutional_education_fit")} />
          <ScaleField label="Decision-maker access" name="accessibility_of_decision_maker" defaultValue={scale("accessibility_of_decision_maker")} />
          <ScaleField label="Warm intro strength" name="warm_intro_strength" defaultValue={scale("warm_intro_strength", 1)} />
          <ScaleField label="Urgency" name="urgency" defaultValue={scale("urgency")} />
          <ScaleField label="Remote/fractional fit" name="remote_or_fractional_fit" defaultValue={scale("remote_or_fractional_fit")} />
          <label className="flex items-center gap-3 rounded-xl border border-slate-800 bg-slate-950/50 p-4 text-sm text-slate-200">
            <input
              type="checkbox"
              name="remote_friendly"
              defaultChecked={defaultValues?.remote_friendly ?? true}
              className="h-4 w-4 rounded border-slate-700 bg-slate-950 text-teal-400"
            />
            Remote friendly
          </label>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Pipeline management</CardTitle>
          <CardDescription>Set next actions manually after sending outreach outside the app.</CardDescription>
        </CardHeader>
        <CardContent className="grid gap-4 md:grid-cols-2">
          <SelectField label="Stage" name="stage" defaultValue={value("stage", "Found")} options={PIPELINE_STAGES} />
          <SelectField label="Monthly revenue potential" name="monthly_revenue_potential" defaultValue={value("monthly_revenue_potential")} options={["", ...REVENUE_POTENTIALS]} />
          <SelectField label="Confidence level" name="confidence_level" defaultValue={value("confidence_level", "Medium")} options={["", ...CONFIDENCE_LEVELS]} />
          <Field label="Last contacted date" name="last_contacted_date" type="date" defaultValue={formatDateInput(defaultValues?.last_contacted_date)} />
          <Field label="Follow-up date" name="follow_up_date" type="date" defaultValue={formatDateInput(defaultValues?.follow_up_date)} />
          <Field label="Objection risk" name="objection_risk" defaultValue={value("objection_risk")} placeholder="Budget, timing, no decision maker..." />
          <div className="md:col-span-2">
            <Label htmlFor="next_action">Next action</Label>
            <Textarea id="next_action" name="next_action" defaultValue={value("next_action")} placeholder="Draft founder-to-founder email, ask for intro, send follow-up..." />
          </div>
        </CardContent>
      </Card>

      <div className="flex flex-wrap items-center gap-3">
        <Button type="submit">{submitLabel}</Button>
        <Link href="/leads" className="text-sm font-medium text-slate-400 hover:text-white">
          Cancel
        </Link>
      </div>
    </form>
  );
}

function Field({
  label,
  name,
  type = "text",
  defaultValue,
  placeholder,
  required,
}: {
  label: string;
  name: string;
  type?: string;
  defaultValue?: string;
  placeholder?: string;
  required?: boolean;
}) {
  return (
    <div>
      <Label htmlFor={name}>{label}</Label>
      <Input id={name} name={name} type={type} defaultValue={defaultValue} placeholder={placeholder} required={required} />
    </div>
  );
}

function SelectField({
  label,
  name,
  defaultValue,
  options,
}: {
  label: string;
  name: string;
  defaultValue?: string;
  options: readonly string[];
}) {
  return (
    <div>
      <Label htmlFor={name}>{label}</Label>
      <select
        id={name}
        name={name}
        defaultValue={defaultValue}
        className="flex h-10 w-full rounded-lg border border-slate-700 bg-slate-950/70 px-3 py-2 text-sm text-slate-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-teal-400"
      >
        {options.map((option) => (
          <option key={option || "empty"} value={option}>
            {option || "Not set"}
          </option>
        ))}
      </select>
    </div>
  );
}

function ScaleField({ label, name, defaultValue }: { label: string; name: string; defaultValue: number }) {
  return (
    <div>
      <Label htmlFor={name}>{label}</Label>
      <select
        id={name}
        name={name}
        defaultValue={defaultValue}
        className="flex h-10 w-full rounded-lg border border-slate-700 bg-slate-950/70 px-3 py-2 text-sm text-slate-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-teal-400"
      >
        {[1, 2, 3, 4, 5].map((value) => (
          <option key={value} value={value}>
            {value} {value === 1 ? "Low" : value === 5 ? "High" : ""}
          </option>
        ))}
      </select>
    </div>
  );
}
