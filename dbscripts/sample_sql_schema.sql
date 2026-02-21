```sql
-- sample_schema_with_sps.sql
-- 20 tables + sample inserts + 5 stored procedures to test relation inference

-- 1. master customer / client tables
CREATE TABLE customer (
  CUST_ID SERIAL PRIMARY KEY,
  CUST_NAME TEXT,
  CUST_TYPE TEXT,
  CUST_STATUS_KEY VARCHAR(50)
);

CREATE TABLE sdo_clientstatustranslation (
  CST_CLIENTSTATUSKEY VARCHAR(50) PRIMARY KEY,
  CST_CLIENTSTATUSDESCRIPTION TEXT
);

-- 2. insurer / carrier
CREATE TABLE insurer (
  INS_ID SERIAL PRIMARY KEY,
  INS_NAME TEXT,
  INS_CODE VARCHAR(50)
);

-- 3. policies / agreements
CREATE TABLE policies (
  POL_POLICYID SERIAL PRIMARY KEY,
  POL_POLICYNO VARCHAR(50),
  POL_CUSTOMERID INT,
  POL_INSURERID INT,
  POL_STATUSKEY VARCHAR(50),
  POL_EFFECTIVE_DATE DATE,
  POL_EXPIRY_DATE DATE
);

CREATE TABLE agreement (
  AGR_ID SERIAL PRIMARY KEY,
  AGR_POLICYID INT,
  AGR_RENEWALDATE DATE,
  AGR_STATUS VARCHAR(30)
);

-- 4. coverage and policy_coverage (many-to-many)
CREATE TABLE coverage (
  COV_CODE VARCHAR(50) PRIMARY KEY,
  COV_DESC TEXT
);

CREATE TABLE policy_coverage (
  PCOV_ID SERIAL PRIMARY KEY,
  PCOV_POLICYID INT,
  PCOV_COVERAGECODE VARCHAR(50),
  PCOV_LIMIT NUMERIC(12,2)
);

-- 5. claims and claim status translation
CREATE TABLE claims (
  CLM_ID SERIAL PRIMARY KEY,
  CLM_POLICYID INT,
  CLM_CLAIMNO VARCHAR(50),
  CLM_CLAIMSTATUS VARCHAR(50),
  CLM_RISKID INT,
  CLM_INSURERID INT,
  CLM_LOSS_DATE DATE
);

CREATE TABLE sdo_claimstatus (
  CST_STATUSCODE VARCHAR(50) PRIMARY KEY,
  CST_STATUSDESC TEXT
);

-- 6. risk
CREATE TABLE risk (
  RISK_ID SERIAL PRIMARY KEY,
  RISK_CODE VARCHAR(50),
  RISK_DESC TEXT
);

-- 7. payments and allocations
CREATE TABLE payments (
  PAY_ID SERIAL PRIMARY KEY,
  PAY_POLICYID INT,
  PAY_AMOUNT NUMERIC(12,2),
  PAY_DATE DATE,
  PAY_ALLOCATIONID INT
);

CREATE TABLE payments_allocation (
  ALLOC_ID SERIAL PRIMARY KEY,
  ALLOC_DESC TEXT,
  ALLOC_AMOUNT NUMERIC(12,2)
);

-- 8. invoices
CREATE TABLE invoices (
  INV_ID SERIAL PRIMARY KEY,
  INV_POLICYID INT,
  INV_AMOUNT NUMERIC(12,2),
  INV_DATE DATE
);

-- 9. notes and claim_notes (polymorphic entity id)
CREATE TABLE notes (
  NOTE_ID SERIAL PRIMARY KEY,
  NOTE_ENTITYTYPE VARCHAR(50), -- 'CLAIM','POLICY','CUSTOMER'
  NOTE_ENTITYID INT,
  NOTE_TEXT TEXT,
  NOTE_CREATED DATE
);

CREATE TABLE claim_notes (
  CN_ID SERIAL PRIMARY KEY,
  CN_CLMID INT,
  CN_TEXT TEXT,
  CN_CREATED DATE
);

-- 10. projects and tasks (example unrelated domain but related by customer)
CREATE TABLE projects (
  PRJ_ID SERIAL PRIMARY KEY,
  PRJ_NAME TEXT,
  PRJ_OWNERID INT -- references customer.CUST_ID
);

CREATE TABLE project_tasks (
  TASK_ID SERIAL PRIMARY KEY,
  TASK_PRJID INT,
  TASK_NAME TEXT,
  TASK_STATUS VARCHAR(30)
);

-- 11. contacts and addresses
CREATE TABLE contacts (
  CNT_ID SERIAL PRIMARY KEY,
  CNT_CUSTOMERID INT,
  CNT_NAME TEXT,
  CNT_EMAIL TEXT
);

CREATE TABLE addresses (
  ADDR_ID SERIAL PRIMARY KEY,
  ADDR_CUSTOMERID INT,
  ADDR_LINE1 TEXT,
  ADDR_CITY TEXT,
  ADDR_POSTCODE VARCHAR(20)
);

-- 12. sdo lookup for policy status translations
CREATE TABLE sdo_policystatus (
  PST_STATUSKEY VARCHAR(50) PRIMARY KEY,
  PST_STATUSDESC TEXT
);

-- 13. small audit/log table
CREATE TABLE audit_log (
  AUD_ID SERIAL PRIMARY KEY,
  AUD_ENTITY VARCHAR(50),
  AUD_ENTITYID INT,
  AUD_ACTION VARCHAR(50),
  AUD_TS TIMESTAMP DEFAULT now()
);

-- -------------------------
-- Stored Procedures (5) with joins and complex logic
-- -------------------------

-- SP1: Get policies due next month for a given insurer name (uses agreement.renewaldate)
CREATE OR REPLACE FUNCTION sp_policies_due_next_month(insurer_name TEXT)
RETURNS TABLE(policy_id INT, policy_no TEXT, renewal_date DATE, status TEXT) AS $$
BEGIN
  RETURN QUERY
  SELECT p.pol_policyid, p.pol_policyno, a.agr_renewaldate, p.pol_statuskey
  FROM policies p
  JOIN agreement a ON p.pol_policyid = a.agr_policyid
  JOIN insurer i ON p.pol_insurerid = i.ins_id
  WHERE i.ins_name ILIKE '%' || insurer_name || '%'
    AND a.agr_renewaldate BETWEEN date_trunc('month', current_date + interval '1 month') 
                              AND (date_trunc('month', current_date + interval '1 month') + interval '1 month - 1 day');
END;
$$ LANGUAGE plpgsql;

-- SP2: Aggregate claims per policy and insert summary into audit_log (INSERT ... SELECT with JOIN)
CREATE OR REPLACE FUNCTION sp_aggregate_claims_to_audit()
RETURNS VOID AS $$
BEGIN
  INSERT INTO audit_log (AUD_ENTITY, AUD_ENTITYID, AUD_ACTION)
  SELECT 'policy', c.clm_policyid, 'claims_count:' || COUNT(*)::text
  FROM claims c
  GROUP BY c.clm_policyid;
END;
$$ LANGUAGE plpgsql;

-- SP3: Create claim notes from notes table for any notes referencing claims (INSERT ... SELECT with JOIN)
CREATE OR REPLACE FUNCTION sp_sync_notes_to_claim_notes()
RETURNS INT AS $$
DECLARE
  inserted_count INT := 0;
BEGIN
  INSERT INTO claim_notes (CN_CLMID, CN_TEXT, CN_CREATED)
  SELECT n.note_entityid::int, n.note_text, n.note_created
  FROM notes n
  WHERE n.note_entitytype = 'CLAIM'
    AND NOT EXISTS (
      SELECT 1 FROM claim_notes cn WHERE cn.cn_clmid = n.note_entityid AND cn.cn_text = n.note_text
    );
  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$$ LANGUAGE plpgsql;

-- SP4: Update payments using allocations (UPDATE ... FROM)
CREATE OR REPLACE FUNCTION sp_apply_allocations()
RETURNS INT AS $$
DECLARE
  updated_count INT := 0;
BEGIN
  UPDATE payments p
  SET pay_allocationid = a.alloc_id
  FROM payments_allocation a
  WHERE p.pay_amount = a.alloc_amount
    AND p.pay_allocationid IS NULL;
  GET DIAGNOSTICS updated_count = ROW_COUNT;
  RETURN updated_count;
END;
$$ LANGUAGE plpgsql;

-- SP5: Populate a summary table of policy coverage limits (INSERT ... SELECT with JOINs)
CREATE TABLE policy_coverage_summary (
  PCS_ID SERIAL PRIMARY KEY,
  PCS_POLICYID INT,
  PCS_TOTAL_LIMIT NUMERIC(14,2),
  PCS_COMPUTED_DATE DATE
);

CREATE OR REPLACE FUNCTION sp_compute_policy_coverage_summary()
RETURNS VOID AS $$
BEGIN
  INSERT INTO policy_coverage_summary (PCS_POLICYID, PCS_TOTAL_LIMIT, PCS_COMPUTED_DATE)
  SELECT pcov.pcov_policyid, SUM(pcov.pcov_limit) AS total_limit, current_date
  FROM policy_coverage pcov
  JOIN coverage cov ON pcov.pcov_coveragecode = cov.cov_code
  GROUP BY pcov.pcov_policyid;
END;
$$ LANGUAGE plpgsql;

-- -------------------------
-- Additional stored-proc style queries (dynamic SQL examples)
-- -------------------------

-- Example dynamic SQL inside a procedure (lower confidence for parser)
CREATE OR REPLACE FUNCTION sp_dynamic_example(tablename TEXT)
RETURNS VOID AS $$
DECLARE
  sql TEXT;
BEGIN
  sql := 'INSERT INTO audit_log (AUD_ENTITY, AUD_ENTITYID, AUD_ACTION) SELECT ''' || tablename || ''', 0, ''dynamic''';
  EXECUTE sql;
END;
$$ LANGUAGE plpgsql;

-- -------------------------
-- End of sample schema
-- -------------------------
```