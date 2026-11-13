ALTER TABLE "LiteLLM_TagTable" ADD COLUMN IF NOT EXISTS daswork_organization_id text
    REFERENCES "LiteLLM_OrganizationTable"(organization_id);
ALTER TABLE "LiteLLM_TeamMembership" ADD COLUMN IF NOT EXISTS daswork_allowed_models text[] NOT NULL DEFAULT '{}';
CREATE UNIQUE INDEX IF NOT EXISTS "daswork_session_alias" ON "LiteLLM_VerificationToken"(key_alias)
    WHERE key_alias LIKE 'dw-session-%';
