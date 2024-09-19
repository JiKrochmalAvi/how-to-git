create or replace view dw_prod.mart.account_canceling_dim as
with
    source as (
        select
            *
        from
            dw_prod.model.account_canceling_dim
    ),
    curr as (
        select
            *
        from
            source qualify row_number() over (
                partition by
                    CompanyAccountID
                order by
                    _LoadedUTC desc
            ) = 1
    )
select
    c.AccountType as CancelingAccountType,
    c.AccountSystemEmail as CancelingAccountSystemEmail,
    c.AccountSecurityEmail as CancelingAccountSecurityEmail,
    c.AccountName as CancelingAccountName,
    c.AccountLastName as CancelingAccountLastName,
    c.AccountID as CancelingAccountID,
    c.AccountFirstName as CancelingAccountFirstName,
    c.AccountCreatedRaw as CancelingAccountCreatedRaw,
    c.AccountCreated as CancelingAccountCreated,
    (
        s._LoadedUTC = max(s._LoadedUTC) over (
            partition by
                s.CompanyAccountID
        )
    )::int as IsCurrent,
    s._LoadedUTC,
    s._CreatedUTC,
    s._ValidFromUTC,
    s._ValidToUTC,
    c._UpdatedUTC
from
    source s
    left join curr as c using (CompanyAccountID)
