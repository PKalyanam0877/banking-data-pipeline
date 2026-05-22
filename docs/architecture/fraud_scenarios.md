# Fraud Scenarios

This document defines the first synthetic fraud scenarios for the banking data platform.

The goal is to describe expected fraud behavior before implementing detection logic. In production systems, this helps risk, fraud operations, compliance, and data engineering agree on what the platform should detect and why.

## Scenario 1: Account Takeover Before High-Value Ecommerce Transaction

### Business Story

A customer account shows repeated failed login attempts from an unusual location. Shortly after, the account has a successful password reset and a new device registration. A high-value ecommerce card transaction follows.

### Synthetic Customer

- Customer: `cust_100003`
- Account: `acct_200004`
- Card: `card_300003`

### Signals

- Multiple `login_failed` events
- Foreign IP/location: Amsterdam, NL
- `password_reset` event
- `new_device_registered` event
- High-value ecommerce transaction
- Manual keyed entry mode
- Customer/account already marked as restricted in core banking data

### Data Sources

- `banking.digital-activity.login-events.v1`
- `banking.transaction.card-authorizations.v1`
- `banking.cdc.core-banking.public.customers`
- `banking.cdc.core-banking.public.accounts`
- `banking.cdc.core-banking.public.customer_risk_profiles`

### Expected Risk Outcome

High risk. The platform should generate a fraud-risk event because the transaction follows account-takeover indicators.

## Scenario 2: New Account High-Value Crypto Transaction

### Business Story

A newly opened customer account performs a high-value ecommerce transaction at a crypto or quasi-cash merchant. New accounts with limited history are higher risk, especially when paired with high-value financial-services merchants.

### Synthetic Customer

- Customer: `cust_100005`
- Account: `acct_200006`
- Card: `card_300005`

### Signals

- Recently opened customer/account
- High transaction amount: `1800.00`
- Merchant: `Quick Crypto Exchange`
- Merchant category code: `6051`
- Ecommerce channel
- Card not present
- Manual keyed entry mode
- New device activity from Miami

### Data Sources

- `banking.transaction.card-authorizations.v1`
- `banking.digital-activity.login-events.v1`
- `banking.cdc.core-banking.public.customers`
- `banking.cdc.core-banking.public.accounts`

### Expected Risk Outcome

Medium to high risk. The platform should flag the event for additional review or enhanced scoring, especially because the account is new.

## Scenario 3: Restricted Customer Cross-Border Luxury Purchase

### Business Story

A restricted or high-risk customer attempts a high-value cross-border ecommerce purchase at a luxury merchant. The transaction may indicate compromised credentials, mule activity, or unauthorized use.

### Synthetic Customer

- Customer: `cust_100003`
- Account: `acct_200004`
- Card: `card_300003`

### Signals

- Customer status: `restricted`
- Account status: `restricted`
- KYC risk rating: `high`
- Fraud watchlist flag: `true`
- High transaction amount: `2450.00`
- Merchant: `Global Luxury Watches`
- Merchant country: `AE`
- Ecommerce channel
- Card not present
- Manual keyed entry mode

### Data Sources

- `banking.transaction.card-authorizations.v1`
- `banking.cdc.core-banking.public.customers`
- `banking.cdc.core-banking.public.accounts`
- `banking.cdc.core-banking.public.customer_risk_profiles`

### Expected Risk Outcome

High risk. The platform should generate a fraud-risk event because customer risk, account restriction, cross-border activity, and merchant risk all reinforce each other.

## Initial Detection Principles

The first fraud-risk logic should remain simple and explainable.

Recommended starting signals:

- High transaction amount
- Ecommerce or card-not-present channel
- Manual keyed entry mode
- Restricted customer or account status
- High KYC risk rating
- Fraud watchlist flag
- Multiple failed logins
- Password reset before transaction
- New device registration
- Cross-border transaction
- High-risk merchant category

## Production Considerations

Fraud rules should be explainable because analysts need to understand why an alert fired.

Risk scores and rule flags are sensitive. They can affect customer treatment, so they require lineage, auditability, and access controls.

False positives matter. Blocking legitimate customers can damage trust, while false negatives can create financial loss.

Rules should be monitored over time. A sudden spike in alerts may mean fraud activity increased, but it may also mean an upstream data issue changed the input distribution.
