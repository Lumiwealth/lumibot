export type CsvLead = Record<string, string | number | boolean | Date | null | undefined>;

const CSV_COLUMNS = [
  "company_name",
  "category",
  "website",
  "location",
  "contact_name",
  "contact_title",
  "contact_email",
  "linkedin_url",
  "warm_intro_source",
  "estimated_budget",
  "fit_score",
  "priority_level",
  "stage",
  "next_action",
  "follow_up_date",
  "suggested_offer",
  "personalized_angle",
  "monthly_revenue_potential",
  "confidence_level",
  "notes",
];

function escapeCsv(value: unknown) {
  if (value === null || value === undefined) return "";
  const stringValue = value instanceof Date ? value.toISOString() : String(value);
  const escaped = stringValue.replace(/"/g, '""');
  return /[",\n]/.test(escaped) ? `"${escaped}"` : escaped;
}

export function leadsToCsv(leads: CsvLead[]) {
  const header = CSV_COLUMNS.join(",");
  const rows = leads.map((lead) =>
    CSV_COLUMNS.map((column) => escapeCsv(lead[column])).join(","),
  );

  return [header, ...rows].join("\n");
}
