select
    hubspot_contacts.email,
    merged_veeva.user_id is not null as veeva_claims_user,
    merged_veeva.claim_id as veeva_claim_id
from "postgres"."warehouse"."extract_from_hubspot" as hubspot_contacts
left join "postgres"."warehouse"."merge_veeva_sources" as merged_veeva
    on merged_veeva.email = hubspot_contacts.email
