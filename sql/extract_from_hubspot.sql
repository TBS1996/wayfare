SELECT
    id,
    createdat,
    updatedat,
    properties ->> 'firstname' AS firstname,
    properties ->> 'lastname' AS lastname,
    properties ->> 'email' AS email
FROM
    "postgres"."warehouse"."hubspot__contacts"
