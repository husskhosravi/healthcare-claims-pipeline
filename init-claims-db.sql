-- Create the claims database
CREATE DATABASE claims;

-- Connect to the claims database
\c claims

-- Create the raw_inpatient_claims table (simplified schema for disk space efficiency)
CREATE TABLE IF NOT EXISTS raw_inpatient_claims (
    "DESYNPUF_ID" VARCHAR(255),
    "CLM_ID" VARCHAR(255),
    "SEGMENT" VARCHAR(255),
    "CLM_FROM_DT" DATE,
    "CLM_THRU_DT" DATE,
    "PRVDR_NUM" VARCHAR(255),
    "CLM_PMT_AMT" NUMERIC,
    "NCH_PRMRY_PYR_CLM_PD_AMT" NUMERIC,
    "AT_PHYSN_NPI" VARCHAR(255),
    "OP_PHYSN_NPI" VARCHAR(255),
    "OT_PHYSN_NPI" VARCHAR(255),
    "CLM_ADMSN_DT" DATE,
    "ADMTNG_ICD9_DGNS_CD" VARCHAR(255),
    "CLM_PASS_THRU_PER_DIEM_AMT" NUMERIC,
    "NCH_BENE_IP_DDCTBL_AMT" NUMERIC,
    "NCH_BENE_PTA_COINSRNC_LBLTY_AM" NUMERIC,
    "NCH_BENE_BLOOD_DDCTBL_LBLTY_AM" NUMERIC,
    "CLM_UTLZTN_DAY_CNT" INTEGER,
    "NCH_BENE_DSCHRG_DT" DATE,
    "CLM_DRG_CD" VARCHAR(255),
    "ICD9_DGNS_CD_1" VARCHAR(255),
    "ICD9_DGNS_CD_2" VARCHAR(255),
    "ICD9_DGNS_CD_3" VARCHAR(255),
    processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create the patient_claims_plus table (analytics view)
CREATE TABLE IF NOT EXISTS patient_claims_plus (
    claim_id VARCHAR(255),
    patient_id VARCHAR(255),
    claim_start_date DATE,
    claim_end_date DATE,
    provider_id VARCHAR(255),
    payment_amount NUMERIC,
    admission_date DATE,
    length_of_stay INTEGER,
    total_cost NUMERIC,
    primary_diagnosis VARCHAR(255),
    procedure_count INTEGER,
    diagnosis_count INTEGER,
    is_emergency BOOLEAN,
    year INTEGER,
    month INTEGER,
    processed_date TIMESTAMP
);

-- Create a user for the application to connect with
CREATE USER claims_user WITH PASSWORD 'claims_password';

-- Grant necessary permissions
GRANT ALL PRIVILEGES ON DATABASE claims TO claims_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO claims_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO claims_user;

-- Create useful views for analysis

-- High-cost claims
CREATE VIEW high_cost_claims AS
SELECT 
    claim_id,
    patient_id,
    provider_id,
    total_cost,
    length_of_stay,
    primary_diagnosis,
    admission_date
FROM 
    patient_claims_plus
WHERE 
    total_cost > 5000
ORDER BY 
    total_cost DESC;

-- Emergency admissions
CREATE VIEW emergency_admissions AS
SELECT 
    claim_id,
    patient_id,
    admission_date,
    primary_diagnosis,
    length_of_stay,
    total_cost
FROM 
    patient_claims_plus
WHERE 
    is_emergency = TRUE
ORDER BY 
    admission_date DESC;

-- Monthly claims trend
CREATE VIEW monthly_claims_trend AS
SELECT 
    year,
    month,
    COUNT(*) as claim_count,
    SUM(total_cost) as total_cost,
    AVG(total_cost) as avg_claim_cost,
    SUM(CASE WHEN is_emergency THEN 1 ELSE 0 END) as emergency_count
FROM 
    patient_claims_plus
GROUP BY 
    year, month
ORDER BY 
    year, month;

-- Grant access to views
GRANT SELECT ON high_cost_claims TO claims_user;
GRANT SELECT ON emergency_admissions TO claims_user;
GRANT SELECT ON monthly_claims_trend TO claims_user;

-- Create indexes for performance
CREATE INDEX idx_patientid ON patient_claims_plus(patient_id);
CREATE INDEX idx_year_month ON patient_claims_plus(year, month);
