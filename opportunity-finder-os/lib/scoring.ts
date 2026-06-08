import { SCORE_LABELS, SCORE_WEIGHTS } from "@/lib/constants";
import { clampScore } from "@/lib/utils";

export type ScoreInput = {
  ability_to_pay: number;
  fit_with_my_background: number;
  need_for_ai_product_help: number;
  relevance_to_lcs: number;
  accessibility_of_decision_maker: number;
  warm_intro_strength: number;
  urgency: number;
  remote_or_fractional_fit: number;
};

export type ScoreFactor = keyof typeof SCORE_WEIGHTS;

export function calculateFitScore(input: ScoreInput) {
  const weighted = Object.entries(SCORE_WEIGHTS).reduce((sum, [key, weight]) => {
    const factor = key as ScoreFactor;
    return sum + (clampScore(input[factor]) / 5) * weight;
  }, 0);

  return Math.round(weighted * 100);
}

export function getPriorityLevel(score: number) {
  if (score >= 85) return "Must Contact";
  if (score >= 70) return "Strong Lead";
  if (score >= 55) return "Worth Testing";
  if (score >= 40) return "Low Priority";
  return "Ignore for Now";
}

export function getScoreBreakdown(input: ScoreInput) {
  return Object.entries(SCORE_WEIGHTS).map(([key, weight]) => {
    const factor = key as ScoreFactor;
    const value = clampScore(input[factor]);
    const points = Math.round((value / 5) * weight * 100);

    return {
      key: factor,
      label: SCORE_LABELS[factor],
      value,
      weight,
      points,
    };
  });
}

export function buildScoreInput<T extends ScoreInput>(lead: T): ScoreInput {
  return {
    ability_to_pay: lead.ability_to_pay,
    fit_with_my_background: lead.fit_with_my_background,
    need_for_ai_product_help: lead.need_for_ai_product_help,
    relevance_to_lcs: lead.relevance_to_lcs,
    accessibility_of_decision_maker: lead.accessibility_of_decision_maker,
    warm_intro_strength: lead.warm_intro_strength,
    urgency: lead.urgency,
    remote_or_fractional_fit: lead.remote_or_fractional_fit,
  };
}

export function enrichLeadScore<T extends ScoreInput>(lead: T) {
  const fit_score = calculateFitScore(lead);
  const priority_level = getPriorityLevel(fit_score);

  return {
    fit_score,
    priority_level,
  };
}
