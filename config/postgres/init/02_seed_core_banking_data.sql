INSERT INTO customers (
    customer_id,
    first_name,
    last_name,
    email,
    phone_number,
    date_of_birth,
    ssn_last4,
    address_line1,
    city,
    state_code,
    postal_code,
    country_code,
    customer_status,
    created_at,
    updated_at
) VALUES
    ('cust_100001', 'Ava', 'Patel', 'ava.patel@example.com', '312-555-0101', '1988-04-12', '1042', '110 W Madison St', 'Chicago', 'IL', '60602', 'US', 'active', '2024-01-15 09:12:00', '2026-05-19 08:00:00'),
    ('cust_100002', 'Marcus', 'Johnson', 'marcus.johnson@example.com', '312-555-0102', '1979-11-03', '2841', '450 N State St', 'Chicago', 'IL', '60654', 'US', 'active', '2023-07-20 14:32:00', '2026-05-19 08:00:00'),
    ('cust_100003', 'Sofia', 'Garcia', 'sofia.garcia@example.com', '773-555-0103', '1992-08-25', '9327', '800 W Lake St', 'Chicago', 'IL', '60607', 'US', 'restricted', '2026-05-10 16:45:00', '2026-05-19 08:00:00'),
    ('cust_100004', 'Noah', 'Kim', 'noah.kim@example.com', '847-555-0104', '1984-02-17', '7712', '2100 Central Rd', 'Evanston', 'IL', '60201', 'US', 'active', '2021-03-08 11:05:00', '2026-05-19 08:00:00'),
    ('cust_100005', 'Mia', 'Williams', 'mia.williams@example.com', '224-555-0105', '1996-12-09', '5188', '72 S Main St', 'Naperville', 'IL', '60540', 'US', 'active', '2026-05-18 10:20:00', '2026-05-19 08:00:00')
ON CONFLICT (customer_id) DO NOTHING;

INSERT INTO accounts (
    account_id,
    customer_id,
    account_number,
    routing_number,
    account_type,
    account_status,
    opened_at,
    closed_at,
    created_at,
    updated_at
) VALUES
    ('acct_200001', 'cust_100001', '900000100001', '071000013', 'checking', 'open', '2024-01-15 09:30:00', NULL, '2024-01-15 09:30:00', '2026-05-19 08:00:00'),
    ('acct_200002', 'cust_100001', '900000100002', '071000013', 'savings', 'open', '2024-02-01 10:00:00', NULL, '2024-02-01 10:00:00', '2026-05-19 08:00:00'),
    ('acct_200003', 'cust_100002', '900000100003', '071000013', 'checking', 'open', '2023-07-20 15:00:00', NULL, '2023-07-20 15:00:00', '2026-05-19 08:00:00'),
    ('acct_200004', 'cust_100003', '900000100004', '071000013', 'checking', 'restricted', '2026-05-10 17:00:00', NULL, '2026-05-10 17:00:00', '2026-05-19 08:00:00'),
    ('acct_200005', 'cust_100004', '900000100005', '071000013', 'money_market', 'open', '2021-03-08 11:30:00', NULL, '2021-03-08 11:30:00', '2026-05-19 08:00:00'),
    ('acct_200006', 'cust_100005', '900000100006', '071000013', 'checking', 'open', '2026-05-18 10:45:00', NULL, '2026-05-18 10:45:00', '2026-05-19 08:00:00')
ON CONFLICT (account_id) DO NOTHING;

INSERT INTO account_balances (
    balance_id,
    account_id,
    available_balance,
    current_balance,
    currency,
    balance_as_of,
    created_at
) VALUES
    ('bal_300001', 'acct_200001', 4825.75, 4950.75, 'USD', '2026-05-19 08:00:00', '2026-05-19 08:00:00'),
    ('bal_300002', 'acct_200002', 18500.00, 18500.00, 'USD', '2026-05-19 08:00:00', '2026-05-19 08:00:00'),
    ('bal_300003', 'acct_200003', 925.20, 925.20, 'USD', '2026-05-19 08:00:00', '2026-05-19 08:00:00'),
    ('bal_300004', 'acct_200004', 120.40, 320.40, 'USD', '2026-05-19 08:00:00', '2026-05-19 08:00:00'),
    ('bal_300005', 'acct_200005', 76000.15, 76000.15, 'USD', '2026-05-19 08:00:00', '2026-05-19 08:00:00'),
    ('bal_300006', 'acct_200006', 250.00, 250.00, 'USD', '2026-05-19 08:00:00', '2026-05-19 08:00:00')
ON CONFLICT (balance_id) DO NOTHING;

INSERT INTO customer_risk_profiles (
    risk_profile_id,
    customer_id,
    kyc_status,
    kyc_risk_rating,
    sanctions_screening_status,
    pep_flag,
    fraud_watchlist_flag,
    risk_score,
    updated_at
) VALUES
    ('risk_400001', 'cust_100001', 'verified', 'low', 'clear', FALSE, FALSE, 12.50, '2026-05-19 08:00:00'),
    ('risk_400002', 'cust_100002', 'verified', 'medium', 'clear', FALSE, FALSE, 38.00, '2026-05-19 08:00:00'),
    ('risk_400003', 'cust_100003', 'pending', 'high', 'potential_match', FALSE, TRUE, 87.25, '2026-05-19 08:00:00'),
    ('risk_400004', 'cust_100004', 'verified', 'low', 'clear', FALSE, FALSE, 8.75, '2026-05-19 08:00:00'),
    ('risk_400005', 'cust_100005', 'pending', 'medium', 'clear', FALSE, FALSE, 52.30, '2026-05-19 08:00:00')
ON CONFLICT (risk_profile_id) DO NOTHING;
