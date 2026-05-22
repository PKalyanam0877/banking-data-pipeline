CREATE TABLE customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    phone_number VARCHAR(30),
    date_of_birth DATE NOT NULL,
    ssn_last4 CHAR(4),
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    city VARCHAR(100),
    state_code CHAR(2),
    postal_code VARCHAR(20),
    country_code CHAR(2) NOT NULL DEFAULT 'US',
    customer_status VARCHAR(30) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT customers_status_chk CHECK (
        customer_status IN ('active', 'inactive', 'closed', 'restricted')
    )
);

CREATE TABLE accounts (
    account_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL REFERENCES customers(customer_id),
    account_number VARCHAR(30) NOT NULL UNIQUE,
    routing_number CHAR(9) NOT NULL,
    account_type VARCHAR(30) NOT NULL,
    account_status VARCHAR(30) NOT NULL,
    opened_at TIMESTAMP NOT NULL,
    closed_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT accounts_type_chk CHECK (
        account_type IN ('checking', 'savings', 'money_market')
    ),
    CONSTRAINT accounts_status_chk CHECK (
        account_status IN ('open', 'closed', 'frozen', 'restricted')
    )
);

CREATE TABLE account_balances (
    balance_id VARCHAR(50) PRIMARY KEY,
    account_id VARCHAR(50) NOT NULL REFERENCES accounts(account_id),
    available_balance NUMERIC(18,2) NOT NULL,
    current_balance NUMERIC(18,2) NOT NULL,
    currency CHAR(3) NOT NULL DEFAULT 'USD',
    balance_as_of TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT account_balances_currency_chk CHECK (currency = 'USD')
);

CREATE TABLE customer_risk_profiles (
    risk_profile_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL UNIQUE REFERENCES customers(customer_id),
    kyc_status VARCHAR(30) NOT NULL,
    kyc_risk_rating VARCHAR(30) NOT NULL,
    sanctions_screening_status VARCHAR(30) NOT NULL,
    pep_flag BOOLEAN NOT NULL DEFAULT FALSE,
    fraud_watchlist_flag BOOLEAN NOT NULL DEFAULT FALSE,
    risk_score NUMERIC(5,2) NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT risk_profiles_kyc_status_chk CHECK (
        kyc_status IN ('pending', 'verified', 'failed', 'expired')
    ),
    CONSTRAINT risk_profiles_rating_chk CHECK (
        kyc_risk_rating IN ('low', 'medium', 'high')
    ),
    CONSTRAINT risk_profiles_sanctions_chk CHECK (
        sanctions_screening_status IN ('clear', 'potential_match', 'confirmed_match')
    ),
    CONSTRAINT risk_profiles_score_chk CHECK (
        risk_score >= 0 AND risk_score <= 100
    )
);

CREATE INDEX idx_accounts_customer_id ON accounts(customer_id);
CREATE INDEX idx_account_balances_account_id ON account_balances(account_id);
CREATE INDEX idx_customer_risk_profiles_customer_id ON customer_risk_profiles(customer_id);
