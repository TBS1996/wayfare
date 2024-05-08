SELECT
    claims.id AS claim_id,
    users.id AS user_id,
    users.email__sys AS email
FROM
    "postgres"."warehouse"."veeva__claim__v" as claims
    LEFT JOIN "postgres"."warehouse"."veeva__user__sys" AS users ON users.id = claims.created_by__v
