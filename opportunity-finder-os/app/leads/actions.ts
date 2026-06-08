"use server";

import { revalidatePath } from "next/cache";
import { redirect } from "next/navigation";
import { prisma } from "@/lib/prisma";
import { getRecommendedOffer } from "@/lib/outreach";
import { recommendRevenuePotential } from "@/lib/revenue";
import { calculateFitScore, getPriorityLevel } from "@/lib/scoring";
import { formDataToObject, leadFormSchema, stageUpdateSchema } from "@/lib/validations";

export async function createLead(formData: FormData) {
  const parsed = leadFormSchema.parse(formDataToObject(formData));
  const fit_score = calculateFitScore(parsed);
  const priority_level = getPriorityLevel(fit_score);
  const suggested_offer = getRecommendedOffer(parsed.category, fit_score);
  const monthly_revenue_potential =
    parsed.monthly_revenue_potential || recommendRevenuePotential(parsed.category, fit_score);

  const lead = await prisma.lead.create({
    data: {
      ...parsed,
      fit_score,
      priority_level,
      suggested_offer,
      monthly_revenue_potential,
    },
  });

  revalidatePath("/");
  revalidatePath("/leads");
  redirect(`/leads/${lead.id}`);
}

export async function updateLead(id: string, formData: FormData) {
  const parsed = leadFormSchema.parse(formDataToObject(formData));
  const fit_score = calculateFitScore(parsed);
  const priority_level = getPriorityLevel(fit_score);
  const suggested_offer = getRecommendedOffer(parsed.category, fit_score);
  const monthly_revenue_potential =
    parsed.monthly_revenue_potential || recommendRevenuePotential(parsed.category, fit_score);

  await prisma.lead.update({
    where: { id },
    data: {
      ...parsed,
      fit_score,
      priority_level,
      suggested_offer,
      monthly_revenue_potential,
    },
  });

  revalidatePath("/");
  revalidatePath("/leads");
  revalidatePath(`/leads/${id}`);
  redirect(`/leads/${id}`);
}

export async function updateLeadStage(id: string, formData: FormData) {
  const parsed = stageUpdateSchema.parse(formDataToObject(formData));

  await prisma.lead.update({
    where: { id },
    data: parsed,
  });

  revalidatePath("/");
  revalidatePath("/leads");
  revalidatePath("/followups");
  revalidatePath(`/leads/${id}`);
}
