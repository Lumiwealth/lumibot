import { z } from "zod";
import { LEAD_CATEGORIES, PIPELINE_STAGES, REVENUE_POTENTIALS } from "@/lib/constants";

const scale = z.coerce.number().int().min(1).max(5);
const optionalText = z.string().trim().optional().transform((value) => value || undefined);
const optionalDate = z.string().optional().transform((value) => (value ? new Date(`${value}T12:00:00`) : undefined));

export const leadFormSchema = z.object({
  company_name: z.string().trim().min(1, "Company name is required"),
  category: z.enum(LEAD_CATEGORIES),
  website: optionalText,
  location: optionalText,
  contact_name: optionalText,
  contact_title: optionalText,
  contact_email: z.string().trim().email().optional().or(z.literal("")).transform((value) => value || undefined),
  linkedin_url: optionalText,
  warm_intro_source: optionalText,
  notes: optionalText,
  estimated_budget: optionalText,
  urgency: scale,
  remote_friendly: z.coerce.boolean().default(false),
  ability_to_pay: scale,
  fit_with_my_background: scale,
  need_for_ai_product_help: scale,
  relevance_to_lcs: scale,
  relevance_to_decision_intelligence: scale,
  family_office_or_wealth_fit: scale,
  institutional_education_fit: scale,
  accessibility_of_decision_maker: scale,
  warm_intro_strength: scale,
  remote_or_fractional_fit: scale,
  stage: z.enum(PIPELINE_STAGES).default("Found"),
  next_action: optionalText,
  last_contacted_date: optionalDate,
  follow_up_date: optionalDate,
  personalized_angle: optionalText,
  monthly_revenue_potential: z.enum(REVENUE_POTENTIALS).optional().or(z.literal("")).transform((value) => value || undefined),
  objection_risk: optionalText,
  confidence_level: z.enum(["Low", "Medium", "High"]).optional().or(z.literal("")).transform((value) => value || undefined),
});

export const stageUpdateSchema = z.object({
  stage: z.enum(PIPELINE_STAGES),
  next_action: optionalText,
  follow_up_date: optionalDate,
  notes: optionalText,
});

export function formDataToObject(formData: FormData) {
  return Object.fromEntries(formData.entries());
}
