CREATE TABLE IF NOT EXISTS "LiteLLM_BudgetTable" (
    budget_id text PRIMARY KEY, max_budget numeric, budget_duration text NOT NULL, budget_reset_at timestamptz NOT NULL,
    created_by text NOT NULL DEFAULT 'daswork', updated_by text NOT NULL DEFAULT 'daswork'
);
CREATE TABLE IF NOT EXISTS "LiteLLM_OrganizationTable" (
    organization_id text PRIMARY KEY, organization_alias text NOT NULL, budget_id text NOT NULL UNIQUE
        REFERENCES "LiteLLM_BudgetTable"(budget_id),
    spend numeric NOT NULL DEFAULT 0, models text[] DEFAULT '{}',
    created_by text NOT NULL DEFAULT 'daswork', updated_by text NOT NULL DEFAULT 'daswork'
);
CREATE TABLE IF NOT EXISTS "LiteLLM_TagTable" (
    tag_name text PRIMARY KEY, budget_id text NOT NULL UNIQUE REFERENCES "LiteLLM_BudgetTable"(budget_id),
    spend numeric NOT NULL DEFAULT 0, models text[] DEFAULT '{}'
);
ALTER TABLE "LiteLLM_TagTable" ADD COLUMN IF NOT EXISTS daswork_organization_id text
    REFERENCES "LiteLLM_OrganizationTable"(organization_id);
CREATE TABLE IF NOT EXISTS "LiteLLM_UserTable" (
    user_id text PRIMARY KEY, organization_id text REFERENCES "LiteLLM_OrganizationTable"(organization_id),
    user_role text NOT NULL, max_budget numeric, budget_duration text NOT NULL, budget_reset_at timestamptz NOT NULL,
    spend numeric NOT NULL DEFAULT 0, teams text[] NOT NULL DEFAULT '{}', models text[] DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS "LiteLLM_TeamTable" (
    team_id text PRIMARY KEY, team_alias text, organization_id text NOT NULL
        REFERENCES "LiteLLM_OrganizationTable"(organization_id),
    models text[] NOT NULL DEFAULT '{}', blocked boolean NOT NULL DEFAULT false,
    members_with_roles jsonb NOT NULL DEFAULT '[]', admins text[] DEFAULT '{}', members text[] DEFAULT '{}',
    max_budget numeric, model_id integer, access_group_ids text[] DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS "LiteLLM_TeamMembership" (
    user_id text REFERENCES "LiteLLM_UserTable"(user_id), team_id text REFERENCES "LiteLLM_TeamTable"(team_id),
    PRIMARY KEY (user_id, team_id)
);
ALTER TABLE "LiteLLM_TeamMembership" ADD COLUMN IF NOT EXISTS daswork_allowed_models text[] NOT NULL DEFAULT '{}';
CREATE TABLE IF NOT EXISTS "LiteLLM_VerificationToken" (
    token text PRIMARY KEY, key_alias text, user_id text NOT NULL REFERENCES "LiteLLM_UserTable"(user_id),
    team_id text NOT NULL REFERENCES "LiteLLM_TeamTable"(team_id),
    organization_id text NOT NULL REFERENCES "LiteLLM_OrganizationTable"(organization_id),
    expires timestamptz NOT NULL, models text[] NOT NULL, blocked boolean NOT NULL,
    allowed_routes text[] NOT NULL, metadata jsonb NOT NULL,
    project_id text, max_budget numeric, budget_duration text, budget_id text, auto_rotate boolean DEFAULT false
);
CREATE UNIQUE INDEX IF NOT EXISTS "daswork_session_alias" ON "LiteLLM_VerificationToken"(key_alias)
    WHERE key_alias LIKE 'dw-session-%';
CREATE INDEX IF NOT EXISTS "daswork_key_member" ON "LiteLLM_VerificationToken"(team_id,user_id);
CREATE TABLE IF NOT EXISTS "LiteLLM_ProxyModelTable" (
    model_id text PRIMARY KEY, model_name text NOT NULL, litellm_params jsonb NOT NULL, model_info jsonb NOT NULL,
    blocked boolean NOT NULL DEFAULT false, created_by text NOT NULL DEFAULT 'daswork',
    updated_by text NOT NULL DEFAULT 'daswork'
);
CREATE UNIQUE INDEX IF NOT EXISTS "daswork_model_name" ON "LiteLLM_ProxyModelTable"(model_name);
CREATE TABLE IF NOT EXISTS "LiteLLM_SpendLogs" (
    request_id text PRIMARY KEY, litellm_call_id text NOT NULL, call_type text NOT NULL, api_key text NOT NULL,
    "startTime" timestamptz NOT NULL, "endTime" timestamptz NOT NULL, spend numeric NOT NULL,
    prompt_tokens int NOT NULL, completion_tokens int NOT NULL, total_tokens int NOT NULL,
    organization_id text NOT NULL, "user" text NOT NULL, team_id text NOT NULL, request_tags jsonb NOT NULL,
    model text NOT NULL, model_id text, model_group text, updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS "daswork_spend_start" ON "LiteLLM_SpendLogs"("startTime" DESC,request_id);
CREATE TABLE IF NOT EXISTS "LiteLLM_BudgetWindowSpend" (
    entity_type text NOT NULL, entity_id text NOT NULL, window_duration text NOT NULL, window_start timestamptz NOT NULL,
    spend numeric NOT NULL, updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (entity_type, entity_id, window_duration)
);
