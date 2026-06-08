-- CreateTable
CREATE TABLE "Lead" (
    "id" TEXT NOT NULL PRIMARY KEY,
    "company_name" TEXT NOT NULL,
    "category" TEXT NOT NULL,
    "website" TEXT,
    "location" TEXT,
    "contact_name" TEXT,
    "contact_title" TEXT,
    "contact_email" TEXT,
    "linkedin_url" TEXT,
    "warm_intro_source" TEXT,
    "notes" TEXT,
    "estimated_budget" TEXT,
    "urgency" INTEGER NOT NULL DEFAULT 3,
    "remote_friendly" BOOLEAN NOT NULL DEFAULT true,
    "ability_to_pay" INTEGER NOT NULL DEFAULT 3,
    "fit_with_my_background" INTEGER NOT NULL DEFAULT 3,
    "need_for_ai_product_help" INTEGER NOT NULL DEFAULT 3,
    "relevance_to_lcs" INTEGER NOT NULL DEFAULT 3,
    "relevance_to_decision_intelligence" INTEGER NOT NULL DEFAULT 3,
    "family_office_or_wealth_fit" INTEGER NOT NULL DEFAULT 3,
    "institutional_education_fit" INTEGER NOT NULL DEFAULT 3,
    "accessibility_of_decision_maker" INTEGER NOT NULL DEFAULT 3,
    "warm_intro_strength" INTEGER NOT NULL DEFAULT 1,
    "remote_or_fractional_fit" INTEGER NOT NULL DEFAULT 3,
    "fit_score" INTEGER NOT NULL DEFAULT 0,
    "stage" TEXT NOT NULL DEFAULT 'Found',
    "next_action" TEXT,
    "last_contacted_date" DATETIME,
    "follow_up_date" DATETIME,
    "suggested_offer" TEXT,
    "personalized_angle" TEXT,
    "monthly_revenue_potential" TEXT,
    "objection_risk" TEXT,
    "confidence_level" TEXT,
    "priority_level" TEXT NOT NULL DEFAULT 'Worth Testing',
    "created_at" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" DATETIME NOT NULL
);

-- CreateIndex
CREATE INDEX "Lead_category_idx" ON "Lead"("category");

-- CreateIndex
CREATE INDEX "Lead_stage_idx" ON "Lead"("stage");

-- CreateIndex
CREATE INDEX "Lead_priority_level_idx" ON "Lead"("priority_level");

-- CreateIndex
CREATE INDEX "Lead_follow_up_date_idx" ON "Lead"("follow_up_date");

-- CreateIndex
CREATE INDEX "Lead_fit_score_idx" ON "Lead"("fit_score");
